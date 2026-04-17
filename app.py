
from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import urllib.parse
from datetime import datetime
import io
import re

app = Flask(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 20


def find_sds_pdf_urls(product_name, brand, manufacturer):
    pdf_urls = []
    try:
        mfr = manufacturer if manufacturer else brand
        queries = [
            f'"{mfr}" "{product_name}" safety data sheet filetype:pdf',
            f'{mfr} {product_name} SDS MSDS filetype:pdf',
            f'{mfr} {product_name} "safety data sheet" pdf',
            f'site:{mfr.lower().replace(" ", "")}.com {product_name} SDS',
        ]
        for query in queries:
            url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "uddg=" in href:
                    try:
                        href = urllib.parse.unquote(href.split("uddg=")[1].split("&")[0])
                    except Exception:
                        continue
                if (
                    href.startswith("http")
                    and "duckduckgo.com" not in href
                    and (
                        href.lower().endswith(".pdf")
                        or "sds" in href.lower()
                        or "msds" in href.lower()
                        or "safety-data" in href.lower()
                        or "safetydatasheet" in href.lower()
                    )
                ):
                    pdf_urls.append(href)
            if len(pdf_urls) >= 5:
                break
    except Exception as e:
        print(f"DuckDuckGo PDF search error: {e}")
    return list(dict.fromkeys(pdf_urls))[:5]


def extract_pdf_text(pdf_url):
    if not PDF_SUPPORT:
        return None, False
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        content_type = resp.headers.get("Content-Type", "").lower()
        if resp.status_code == 200 and ("pdf" in content_type or pdf_url.lower().endswith(".pdf")):
            pdf_bytes = io.BytesIO(resp.content)
            reader = PyPDF2.PdfReader(pdf_bytes)
            text = ""
            for page in reader.pages[:20]:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "
"
            if len(text) > 500:
                return text, True
    except Exception as e:
        print(f"PDF extraction error for {pdf_url}: {e}")
    return None, False



def extract_h_codes(text):
    if not text:
        return []
    pattern = r'(H\d{3}[A-Z]?(?:\+H\d{3}[A-Z]?)*|EUH\d{3}[A-Z]?)'
    codes = list(set(re.findall(pattern, text)))
    return sorted(codes)


def get_static_sources(product_name, brand, manufacturer):
    mfr = manufacturer if manufacturer else brand
    mfr_lower = mfr.lower()
    sources = []
    known_portals = {
        "castrol": f"https://www.castrol.com/en_gb/united-kingdom/home/search.html?q={urllib.parse.quote(product_name + ' SDS')}",
        "shell": "https://www.shell.com/business-customers/lubricants-for-business/resources/sds-search.html",
        "mobil": "https://www.mobil.com/en/lubes/support/sds-search",
        "basf": f"https://sds.basf.com/sds/search?q={urllib.parse.quote(product_name)}",
        "3m": f"https://www.3m.com/3M/en_US/sds-search/results/?q={urllib.parse.quote(product_name)}",
        "henkel": f"https://www.henkel.com/search?q={urllib.parse.quote(product_name + ' SDS')}",
        "dow": f"https://www.dow.com/en-us/search.html#q={urllib.parse.quote(product_name + ' SDS')}",
    }
    for key, url in known_portals.items():
        if key in mfr_lower:
            sources.append({"source": f"Official SDS Portal ({mfr})", "url": url, "confidence": "high", "type": "search_page"})
            break
    sources.append({
        "source": "GESTIS Database (IFA Germany - WGK)",
        "url": f"https://gestis-database.dguv.de/search?searchterm={urllib.parse.quote(brand)}&lang=en",
        "confidence": "high", "type": "search_page"
    })
    sources.append({
        "source": "ECHA Database (EU)",
        "url": f"https://echa.europa.eu/search-for-chemicals?p_p_id=disssubstancesearch_WAR_disssearchportlet&_disssubstancesearch_WAR_disssearchportlet_searchOP=1&_disssubstancesearch_WAR_disssearchportlet_searchKey={urllib.parse.quote(brand)}",
        "confidence": "high", "type": "search_page"
    })
    sources.append({
        "source": "eChemPortal (OECD)",
        "url": f"https://www.echemportal.org/echemportal/substance-search/result?query={urllib.parse.quote(brand)}",
        "confidence": "high", "type": "search_page"
    })
    return sources


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "SDS Search API",
        "version": "5.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "capabilities": [
            "Automatic SDS PDF search",
            "Automatic PDF download and text extraction",
            "H-code extraction from found SDS",
            "SDS validity check",
            "Static authoritative source links"
        ]
    })


@app.route("/search-sds", methods=["GET", "POST"])
def search_sds():
    try:
        if request.method == "POST":
            data = request.get_json() or {}
        else:
            data = request.args

        product_name = data.get("product_name", "").strip()
        brand = data.get("brand", "").strip()
        manufacturer = data.get("manufacturer", "").strip()
        asin = data.get("asin", "").strip()

        if not product_name and not brand:
            return jsonify({"status": "error", "message": "product_name or brand required"}), 400

        pdf_urls = find_sds_pdf_urls(product_name, brand, manufacturer)

        sds_content = None
        sds_pdf_url = None
        h_codes_found = []

        for pdf_url in pdf_urls:
            text, success = extract_pdf_text(pdf_url)
            if success and is_valid_sds(text):
                sds_content = text[:5000]
                sds_pdf_url = pdf_url
                h_codes_found = extract_h_codes(text)
                break

        static_sources = get_static_sources(product_name, brand, manufacturer)

        pdf_results = []
        for url in pdf_urls:
            pdf_results.append({
                "source": "Auto-Found PDF",
                "url": url,
                "confidence": "high" if url.lower().endswith(".pdf") else "medium",
                "type": "pdf"
            })

        all_results = pdf_results + static_sources

        return jsonify({
            "status": "success",
            "query": {
                "product_name": product_name,
                "brand": brand,
                "manufacturer": manufacturer,
                "asin": asin
            },
            "automation_result": {
                "sds_found": sds_content is not None,
                "sds_pdf_url": sds_pdf_url,
                "h_codes_extracted": h_codes_found,
                "sds_content_preview": sds_content[:2000] if sds_content else None,
                "pdf_urls_found": pdf_urls
            },
            "total_results": len(all_results),
            "sds_results": all_results,
            "search_timestamp": datetime.utcnow().isoformat()
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "search_timestamp": datetime.utcnow().isoformat()
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

