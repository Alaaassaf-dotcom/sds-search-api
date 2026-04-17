
from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

app = Flask(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 10  # seconds per request

# ─────────────────────────────────────────────
# SEARCH FUNCTIONS (one per SDS source)
# ─────────────────────────────────────────────

def search_google(product_name, brand, manufacturer):
    """Search Google for SDS PDF links."""
    results = []
    try:
        query = f'"{product_name}" "{brand}" SDS "safety data sheet" filetype:pdf'
        if manufacturer:
            query = f'"{product_name}" "{manufacturer}" SDS "safety data sheet" filetype:pdf'
        
        url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&num=10"
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
        
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/url?q=" in href:
                actual_url = href.split("/url?q=")[1].split("&")[0]
                if actual_url.lower().endswith(".pdf") or "sds" in actual_url.lower():
                    results.append({
                        "source": "Google Search",
                        "url": urllib.parse.unquote(actual_url),
                        "confidence": "medium",
                        "type": "pdf" if actual_url.lower().endswith(".pdf") else "page"
                    })
    except Exception as e:
        print(f"Google search error: {e}")
    return results[:5]


def search_chemicalsafety(product_name, brand, manufacturer):
    """Search ChemicalSafety.com database."""
    results = []
    try:
        search_term = brand if brand else product_name
        url = f"https://chemicalsafety.com/sds-search/?q={urllib.parse.quote(search_term)}"
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
        
        for item in soup.find_all("a", href=True):
            href = item["href"]
            text = item.get_text(strip=True).lower()
            if "sds" in text or "safety" in text or href.endswith(".pdf"):
                full_url = href if href.startswith("http") else f"https://chemicalsafety.com{href}"
                results.append({
                    "source": "ChemicalSafety.com",
                    "url": full_url,
                    "confidence": "high" if brand.lower() in text else "medium",
                    "type": "pdf" if href.endswith(".pdf") else "page"
                })
    except Exception as e:
        print(f"ChemicalSafety search error: {e}")
    return results[:3]


def search_fishersci(product_name, brand, manufacturer):
    """Search Fisher Scientific SDS database."""
    results = []
    try:
        query = f"{product_name} {brand}".strip()
        url = f"https://www.fishersci.com/us/en/catalog/search/sdshome.html?query={urllib.parse.quote(query)}"
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
        
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "sds" in href.lower() or href.endswith(".pdf"):
                full_url = href if href.startswith("http") else f"https://www.fishersci.com{href}"
                results.append({
                    "source": "Fisher Scientific",
                    "url": full_url,
                    "confidence": "high",
                    "type": "pdf" if href.endswith(".pdf") else "page"
                })
    except Exception as e:
        print(f"Fisher Scientific search error: {e}")
    return results[:3]


def search_manufacturer_website(product_name, brand, manufacturer):
    """Search manufacturer's official website for SDS."""
    results = []
    try:
        mfr = manufacturer if manufacturer else brand
        query = f'site:{mfr.lower().replace(" ", "")}.com "{product_name}" SDS filetype:pdf'
        fallback_query = f'"{mfr}" "{product_name}" SDS safety data sheet site:*.com filetype:pdf'
        
        for q in [query, fallback_query]:
            url = f"https://www.google.com/search?q={urllib.parse.quote(q)}&num=5"
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
            
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/url?q=" in href:
                    actual_url = href.split("/url?q=")[1].split("&")[0]
                    actual_url = urllib.parse.unquote(actual_url)
                    if actual_url.endswith(".pdf"):
                        results.append({
                            "source": f"Manufacturer Website ({mfr})",
                            "url": actual_url,
                            "confidence": "high",
                            "type": "pdf"
                        })
            if results:
                break
    except Exception as e:
        print(f"Manufacturer website search error: {e}")
    return results[:3]


def search_echa(product_name, brand, manufacturer):
    """Search ECHA (European Chemicals Agency) database."""
    results = []
    try:
        query = brand if brand else product_name
        url = (
            f"https://echa.europa.eu/search-for-chemicals?"
            f"p_p_id=disssubstancesearch_WAR_disssearchportlet"
            f"&_disssubstancesearch_WAR_disssearchportlet_searchOP=1"
            f"&_disssubstancesearch_WAR_disssearchportlet_searchKey={urllib.parse.quote(query)}"
        )
        results.append({
            "source": "ECHA Database",
            "url": url,
            "confidence": "medium",
            "type": "search_page",
            "note": "Manual review required on ECHA portal"
        })
    except Exception as e:
        print(f"ECHA search error: {e}")
    return results


def deduplicate_results(results):
    """Remove duplicate URLs from results."""
    seen_urls = set()
    unique = []
    for r in results:
        url = r.get("url", "")
        if url not in seen_urls:
            seen_urls.add(url)
            unique.append(r)
    return unique


def rank_results(results):
    """Rank results by confidence: high > medium > low."""
    order = {"high": 0, "medium": 1, "low": 2}
    return sorted(results, key=lambda x: order.get(x.get("confidence", "low"), 2))


# ─────────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "service": "SDS Search API",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat()
    })


@app.route("/search-sds", methods=["POST"])
def search_sds():
    """
    Main SDS search endpoint.

    Request Body:
    {
        "product_name": "string (required)",
        "brand": "string (required)",
        "manufacturer": "string (optional)",
        "asin": "string (optional)",
        "search_scope": "all | google | manufacturer | databases (optional, default: all)"
    }

    Response:
    {
        "status": "success | no_match_found | error",
        "query": { ...input params... },
        "total_results": int,
        "sds_results": [ ... ],
        "search_timestamp": "ISO 8601"
    }
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({"status": "error", "message": "Request body is required"}), 400

        product_name = data.get("product_name", "").strip()
        brand = data.get("brand", "").strip()
        manufacturer = data.get("manufacturer", "").strip()
        asin = data.get("asin", "").strip()
        search_scope = data.get("search_scope", "all").lower()

        if not product_name and not brand:
            return jsonify({
                "status": "error",
                "message": "At least 'product_name' or 'brand' is required"
            }), 400

        # ── Run searches in parallel ──
        all_results = []
        search_functions = []

        if search_scope in ["all", "google"]:
            search_functions.append(("Google", search_google, (product_name, brand, manufacturer)))
        if search_scope in ["all", "databases"]:
            search_functions.append(("ChemicalSafety", search_chemicalsafety, (product_name, brand, manufacturer)))
            search_functions.append(("FisherSci", search_fishersci, (product_name, brand, manufacturer)))
            search_functions.append(("ECHA", search_echa, (product_name, brand, manufacturer)))
        if search_scope in ["all", "manufacturer"]:
            search_functions.append(("Manufacturer", search_manufacturer_website, (product_name, brand, manufacturer)))

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(fn, *args): name
                for name, fn, args in search_functions
            }
            for future in as_completed(futures):
                try:
                    results = future.result(timeout=15)
                    all_results.extend(results)
                except Exception as e:
                    print(f"Search function error: {e}")

        # ── Post-process results ──
        all_results = deduplicate_results(all_results)
        all_results = rank_results(all_results)

        status = "success" if all_results else "no_match_found"

        return jsonify({
            "status": status,
            "query": {
                "product_name": product_name,
                "brand": brand,
                "manufacturer": manufacturer,
                "asin": asin,
                "search_scope": search_scope
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


@app.route("/search-sds", methods=["GET"])
def search_sds_get():
    """GET version for quick testing via browser."""
    product_name = request.args.get("product_name", "")
    brand = request.args.get("brand", "")
    manufacturer = request.args.get("manufacturer", "")

    if not product_name and not brand:
        return jsonify({"status": "error", "message": "product_name or brand required"}), 400

    with app.test_request_context(
        "/search-sds",
        method="POST",
        json={"product_name": product_name, "brand": brand, "manufacturer": manufacturer}
    ):
        return search_sds()


# ─────────────────────────────────────────────
# RUN SERVER
# ─────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

