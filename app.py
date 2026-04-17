from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import urllib.parse
from datetime import datetime

app = Flask(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
TIMEOUT = 15
BLOCKED = ["amazon.com","ebay.com","walmart.com","youtube.com","facebook.com","duckduckgo.com","chemicalsafety.com"]

def find_sds_urls(product_name, brand, manufacturer):
    found = []
    mfr = manufacturer if manufacturer else brand
    queries = [
        mfr + " " + product_name + " safety data sheet filetype:pdf",
        mfr + " " + product_name + " SDS MSDS pdf",
        mfr + " " + product_name + " safety data sheet",
    ]
    for query in queries:
        try:
            url = "https://html.duckduckgo.com/html/?q=" + urllib.parse.quote(query)
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "uddg=" in href:
                    try:
                        href = urllib.parse.unquote(href.split("uddg=").split("&"))
                    except Exception:
                        continue
                if not href.startswith("http"):
                    continue
                if any(d in href.lower() for d in BLOCKED):
                    continue
                h = href.lower()
                if h.endswith(".pdf") or "sds" in h or "msds" in h or "safety-data" in h:
                    found.append(href)
        except Exception:
            continue
        if len(found) >= 5:
            break
    return list(dict.fromkeys(found))[:5]

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "service": "SDS Search API", "version": "5.0.0", "timestamp": datetime.utcnow().isoformat()})

@app.route("/search-sds", methods=["GET", "POST"])
def search_sds():
    try:
        data = request.get_json() or {} if request.method == "POST" else request.args
        product_name = str(data.get("product_name", "")).strip()
        brand = str(data.get("brand", "")).strip()
        manufacturer = str(data.get("manufacturer", "")).strip()
        asin = str(data.get("asin", "")).strip()
        if not product_name and not brand:
            return jsonify({"status": "error", "message": "product_name or brand required"}), 400
        sds_urls = find_sds_urls(product_name, brand, manufacturer)
        results = []
        for url in sds_urls:
            results.append({"source": "Auto-Found SDS", "url": url, "confidence": "high" if url.lower().endswith(".pdf") else "medium", "type": "pdf" if url.lower().endswith(".pdf") else "page"})
        results.append({"source": "GESTIS Database (IFA Germany)", "url": "https://gestis-database.dguv.de/search?searchterm=" + urllib.parse.quote(brand) + "&lang=en", "confidence": "high", "type": "search_page"})
        results.append({"source": "ECHA Database (EU)", "url": "https://echa.europa.eu/search-for-chemicals?p_p_id=disssubstancesearch_WAR_disssearchportlet&_disssubstancesearch_WAR_disssearchportlet_searchOP=1&_disssubstancesearch_WAR_disssearchportlet_searchKey=" + urllib.parse.quote(brand), "confidence": "high", "type": "search_page"})
        return jsonify({"status": "success", "query": {"product_name": product_name, "brand": brand, "manufacturer": manufacturer, "asin": asin}, "auto_search": {"urls_found": len(sds_urls), "pdf_urls": [u for u in sds_urls if u.lower().endswith(".pdf")], "all_urls": sds_urls}, "total_results": len(results), "sds_results": results, "search_timestamp": datetime.utcnow().isoformat()})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
