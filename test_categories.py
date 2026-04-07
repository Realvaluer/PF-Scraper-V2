"""
Quick 1-page test: scrapes page 1 from one target per unique category.
Tests residential rent/sale, commercial rent/sale, and one non-Dubai emirate.
Does NOT write to Supabase — just prints results.

Usage: python -m test_categories
"""

import json
import logging
import random
import time

from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PF_BASE = "https://www.propertyfinder.ae/en"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# One target per unique category to test
TEST_TARGETS = [
    # Residential
    {"url": f"{PF_BASE}/rent/dubai/apartments-for-rent.html", "label": "Dubai Apt Rent (Residential)", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_BASE}/buy/dubai/villas-for-sale.html", "label": "Dubai Villa Sale (Residential)", "stored_type": "sale", "property_type": "villa"},
    {"url": f"{PF_BASE}/buy/dubai/land-for-sale.html", "label": "Dubai Land Sale (Residential)", "stored_type": "sale", "property_type": "land"},
    # Commercial
    {"url": f"{PF_BASE}/commercial-rent/dubai/offices-for-rent.html", "label": "Dubai Office Rent (Commercial)", "stored_type": "rent", "property_type": "office"},
    {"url": f"{PF_BASE}/commercial-buy/dubai/offices-for-sale.html", "label": "Dubai Office Sale (Commercial)", "stored_type": "sale", "property_type": "office"},
    {"url": f"{PF_BASE}/commercial-rent/dubai/warehouses-for-rent.html", "label": "Dubai Warehouse Rent (Commercial)", "stored_type": "rent", "property_type": "warehouse"},
    {"url": f"{PF_BASE}/commercial-rent/dubai/shops-for-rent.html", "label": "Dubai Shop Rent (Commercial)", "stored_type": "rent", "property_type": "shop"},
    {"url": f"{PF_BASE}/commercial-buy/dubai/land-for-sale.html", "label": "Dubai Commercial Land Sale", "stored_type": "sale", "property_type": "commercial land"},
    # Other emirates
    {"url": f"{PF_BASE}/rent/abu-dhabi/apartments-for-rent.html", "label": "Abu Dhabi Apt Rent", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_BASE}/rent/sharjah/apartments-for-rent.html", "label": "Sharjah Apt Rent", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_BASE}/rent/ajman/apartments-for-rent.html", "label": "Ajman Apt Rent", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_BASE}/rent/ras-al-khaimah/apartments-for-rent.html", "label": "RAK Apt Rent", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_BASE}/rent/fujairah/apartments-for-rent.html", "label": "Fujairah Apt Rent", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_BASE}/rent/umm-al-quwain/apartments-for-rent.html", "label": "UAQ Apt Rent", "stored_type": "rent", "property_type": "apartment"},
    # Commercial in other emirate
    {"url": f"{PF_BASE}/commercial-rent/abu-dhabi/offices-for-rent.html", "label": "Abu Dhabi Office Rent (Commercial)", "stored_type": "rent", "property_type": "office"},
]


def extract_count_and_sample(page_content: str) -> dict:
    """Extract listing count and first listing sample from __NEXT_DATA__."""
    result = {"has_next_data": False, "count": 0, "sample": None}

    try:
        start = page_content.find('id="__NEXT_DATA__"')
        if start == -1:
            return result
        result["has_next_data"] = True
        json_start = page_content.find(">", start) + 1
        json_end = page_content.find("</script>", json_start)
        raw_json = page_content[json_start:json_end]
        data = json.loads(raw_json)
    except Exception as e:
        result["error"] = str(e)
        return result

    try:
        page_props = data.get("props", {}).get("pageProps", {})
        search_result = page_props.get("searchResult", {}) or page_props.get("search_result", {}) or {}
        properties = search_result.get("listings", []) or search_result.get("properties", [])
        result["count"] = len(properties)

        if properties:
            prop = properties[0].get("property", properties[0])
            loc = prop.get("location", {})
            loc_name = loc.get("full_name", "")[:60] if isinstance(loc, dict) else ""
            result["sample"] = {
                "ref": prop.get("reference", ""),
                "beds": prop.get("bedrooms"),
                "price": prop.get("price", {}).get("value") if isinstance(prop.get("price"), dict) else prop.get("price"),
                "size": prop.get("size", {}).get("value") if isinstance(prop.get("size"), dict) else prop.get("size"),
                "location": loc_name,
                "listed_date": prop.get("listed_date", ""),
                "last_refreshed": prop.get("last_refreshed_at", ""),
            }
    except Exception as e:
        result["error"] = str(e)

    return result


def run_test():
    logger.info(f"=== Category Test: {len(TEST_TARGETS)} targets, 1 page each ===\n")

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process"],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id="Asia/Dubai",
        )
        page = context.new_page()
        stealth_sync(page)
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        # Pass WAF
        logger.info("Passing WAF challenge...")
        page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(15)
        content = page.content()
        logger.info(f"Homepage length: {len(content)} — {'PASSED' if len(content) > 50000 else 'MAYBE FAILED'}")
        time.sleep(3)

        for idx, target in enumerate(TEST_TARGETS):
            label = target["label"]
            url = target["url"]

            # Re-warm WAF every 5 targets
            if idx > 0 and idx % 5 == 0:
                logger.info("Re-warming WAF...")
                page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
                time.sleep(random.uniform(5, 8))

            logger.info(f"\n[{idx+1}/{len(TEST_TARGETS)}] {label}")
            logger.info(f"  URL: {url}")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(3)

                try:
                    page.wait_for_selector('script#__NEXT_DATA__', timeout=10000)
                except Exception:
                    pass

                content = page.content()
                title = page.title()
                length = len(content)

                # Check for issues
                status = "OK"
                if length < 5000:
                    status = "BLOCKED (WAF)"
                elif "page not found" in content.lower()[:3000] or "404" in title:
                    status = "404"
                elif "no properties found" in content.lower():
                    status = "NO RESULTS"

                info = extract_count_and_sample(content)

                result = {
                    "label": label,
                    "status": status,
                    "title": title[:60],
                    "page_length": length,
                    "has_next_data": info["has_next_data"],
                    "listings": info["count"],
                    "sample": info.get("sample"),
                }
                results.append(result)

                if info["count"] > 0:
                    s = info["sample"]
                    logger.info(f"  ✅ {status} | {info['count']} listings | __NEXT_DATA__={'YES' if info['has_next_data'] else 'NO'}")
                    logger.info(f"  Sample: ref={s['ref']}, beds={s['beds']}, price={s['price']}, listed_date={s['listed_date'][:20]}")
                else:
                    logger.info(f"  {'❌' if status != 'OK' else '⚠️'} {status} | {info['count']} listings | __NEXT_DATA__={'YES' if info['has_next_data'] else 'NO'} | title={title[:50]}")

            except Exception as e:
                logger.error(f"  ❌ ERROR: {e}")
                results.append({"label": label, "status": f"ERROR: {e}", "listings": 0})

            time.sleep(random.uniform(3, 5))

        browser.close()

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    for r in results:
        icon = "✅" if r.get("listings", 0) > 0 else "❌"
        logger.info(f"  {icon} {r['label']:40} | {r.get('status','?'):12} | {r.get('listings',0):3} listings | __NEXT_DATA__={'YES' if r.get('has_next_data') else 'NO'}")
    logger.info("=" * 70)

    ok = sum(1 for r in results if r.get("listings", 0) > 0)
    logger.info(f"\n{ok}/{len(results)} targets returned listings")


if __name__ == "__main__":
    run_test()
