import json
import logging
import random
import re
import time
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from supabase_client import upsert_listings, fetch_current_prices, log_price_changes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SCRAPE_TARGETS = [
    {
        "url": "https://www.propertyfinder.ae/en/rent/dubai/apartments-for-rent.html",
        "label": "Dubai Apartments (rent)",
        "stored_type": "rent",
        "property_type": "apartment",
    },
    {
        "url": "https://www.propertyfinder.ae/en/buy/dubai/apartments-for-sale.html",
        "label": "Dubai Apartments (sale)",
        "stored_type": "sale",
        "property_type": "apartment",
    },
]

MAX_LISTINGS_PER_TARGET = 50

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def extract_listings(page_content: str, stored_type: str, property_type: str) -> list[dict]:
    """Extract listings from __NEXT_DATA__ using actual PF field names."""
    try:
        start = page_content.find('id="__NEXT_DATA__"')
        if start == -1:
            logger.warning("__NEXT_DATA__ not found")
            return []
        json_start = page_content.find(">", start) + 1
        json_end = page_content.find("</script>", json_start)
        raw_json = page_content[json_start:json_end]
        data = json.loads(raw_json)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to parse __NEXT_DATA__: {e}")
        return []

    listings = []
    try:
        page_props = data.get("props", {}).get("pageProps", {})
        logger.info(f"pageProps keys: {list(page_props.keys())[:20]}")
        search_result = page_props.get("searchResult", {})
        if not search_result:
            # Try alternative paths
            search_result = page_props.get("search_result", {}) or page_props.get("data", {}) or page_props.get("results", {})
            logger.info(f"Used fallback searchResult, keys: {list(search_result.keys()) if isinstance(search_result, dict) else 'not a dict'}")

        # PF uses "listings" — fallback to "properties"
        properties = search_result.get("listings", [])
        if not properties:
            properties = search_result.get("properties", [])

        # Debug: log available keys in searchResult
        logger.info(f"searchResult keys: {list(search_result.keys())}")
        logger.info(f"Found {len(properties)} listings in __NEXT_DATA__")

        # Debug: log first listing's key fields
        if properties:
            fp = properties[0].get("property", properties[0])
            loc = fp.get("location", {})
            loc_name = loc.get("full_name", "")[:80] if isinstance(loc, dict) else ""
            logger.info(f"First listing: ref={fp.get('reference')}, beds={fp.get('bedrooms')}, "
                        f"price={fp.get('price')}, size={fp.get('size')}, location={loc_name}")

        for wrapper in properties:
            try:
                # Real data is inside the "property" sub-object
                prop = wrapper.get("property") or wrapper
                if not isinstance(prop, dict):
                    logger.warning("Skipping listing: property is not a dict")
                    continue

                # reference
                reference_no = prop.get("reference", "") or str(prop.get("id", ""))

                # price — could be {"value": X} or just a number
                price_obj = prop.get("price", {})
                if isinstance(price_obj, dict):
                    price = price_obj.get("value", 0) or price_obj.get("amount", 0)
                else:
                    price = price_obj
                try:
                    price = float(price) if price else 0
                except (ValueError, TypeError):
                    price = 0

                # size — could be {"value": X} or just a number
                size_obj = prop.get("size", {}) or prop.get("area", {})
                if isinstance(size_obj, dict):
                    size_sqft = size_obj.get("value", 0) or size_obj.get("sqft", 0)
                else:
                    size_sqft = size_obj
                try:
                    size_sqft = float(size_sqft) if size_sqft else 0
                except (ValueError, TypeError):
                    size_sqft = 0

                # bedrooms — show actual number, "Studio" for 0
                bedrooms_raw = prop.get("bedrooms", 0)
                try:
                    bed_num = int(bedrooms_raw)
                except (ValueError, TypeError):
                    bed_num = -1

                if bed_num == 0 or str(bedrooms_raw).lower() == "studio":
                    bedrooms = "Studio"
                elif bed_num > 0:
                    bedrooms = str(bed_num)
                else:
                    bedrooms = str(bedrooms_raw)

                # location — could be dict or list
                location_obj = prop.get("location", {})
                full_name = ""

                if isinstance(location_obj, dict):
                    full_name = location_obj.get("full_name", "") or location_obj.get("name", "")
                elif isinstance(location_obj, list) and location_obj:
                    location_parts = [
                        loc.get("name", "") or loc.get("full_name", "")
                        for loc in location_obj
                        if isinstance(loc, dict) and (loc.get("name") or loc.get("full_name"))
                    ]
                    full_name = ", ".join(location_parts)

                # Parse community and building from full_name
                # PF format: "Building, Sub-area, Community, City" (city is LAST)
                # e.g. "Claren Tower 2, Claren Towers, Downtown Dubai, Dubai"
                # e.g. "Reef Residence, District 13, Jumeirah Village Circle, Dubai"
                # e.g. "LIVA, Town Square, Dubai"
                # e.g. "Business Bay, Dubai"
                community = ""
                building = ""
                if full_name:
                    parts = [p.strip() for p in full_name.split(",")]
                    if len(parts) >= 4:
                        # "Building, Sub, Community, City"
                        building = parts[0]
                        community = parts[-2]  # second to last = community
                    elif len(parts) == 3:
                        # "Building, Community, City"
                        building = parts[0]
                        community = parts[1]
                    elif len(parts) == 2:
                        # "Community, City"
                        community = parts[0]
                    elif len(parts) == 1:
                        community = parts[0]

                # URL
                listing_url = prop.get("details_path", "") or prop.get("share_url", "") or prop.get("url", "")
                if listing_url and not listing_url.startswith("http"):
                    listing_url = f"https://www.propertyfinder.ae{listing_url}"

                # Price per sqft — PF stores as {"price": 1578, "unit": "sqft"}
                ppa_obj = prop.get("price_per_area", {})
                if isinstance(ppa_obj, dict):
                    price_per_sqft = ppa_obj.get("price", 0)
                else:
                    price_per_sqft = ppa_obj or 0
                if not price_per_sqft and size_sqft and price:
                    price_per_sqft = round(price / size_sqft, 2)
                try:
                    price_per_sqft = float(price_per_sqft) if price_per_sqft else 0
                except (ValueError, TypeError):
                    price_per_sqft = 0

                # Skip listings with no useful data
                if not reference_no and not price and not size_sqft:
                    logger.warning(f"Skipping empty listing")
                    continue

                listings.append({
                    "reference_no": reference_no,
                    "listing_type": stored_type,
                    "property_type": property_type,
                    "community": community,
                    "building": building,
                    "bedrooms": bedrooms,
                    "size_sqft": size_sqft,
                    "price": price,
                    "price_per_sqft": price_per_sqft,
                    "listing_url": listing_url,
                })
            except Exception as e:
                logger.warning(f"Failed to parse listing: {e}")
                continue

    except Exception as e:
        logger.error(f"Failed to extract listings: {e}")

    return listings


def wait_for_page_content(page) -> str:
    """Wait for page content to load."""
    time.sleep(3)

    try:
        page.wait_for_selector('script#__NEXT_DATA__', timeout=10000)
        logger.info("Found __NEXT_DATA__")
        return page.content()
    except Exception:
        pass

    try:
        page.wait_for_selector('[class*="property-card"], [class*="listing"]', timeout=10000)
        logger.info("Found property card elements")
        return page.content()
    except Exception:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    content = page.content()
    logger.info(f"Page content length after all waits: {len(content)}")
    return content


def pass_waf_challenge(page) -> bool:
    """Navigate to PF homepage to pass WAF challenge."""
    logger.info("Navigating to PF homepage to pass WAF challenge...")
    try:
        page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(15)

        content = page.content()
        logger.info(f"Homepage length: {len(content)}")

        if len(content) > 50000:
            logger.info("WAF challenge passed")
            return True

        logger.warning("WAF challenge may not have passed — waiting longer...")
        time.sleep(10)
        content = page.content()
        logger.info(f"Homepage length after extra wait: {len(content)}")

        if len(content) > 50000:
            logger.info("WAF challenge passed (after extra wait)")
            return True

        logger.warning("WAF challenge may not have passed")
        return False
    except Exception as e:
        logger.error(f"WAF challenge failed: {e}")
        return False


def run_scraper():
    start_time = datetime.now(timezone.utc)
    logger.info(f"=== PF Scraper V2 started at {start_time.isoformat()} ===")

    all_listings = []
    total_price_changes = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id="Asia/Dubai",
        )
        page = context.new_page()
        stealth_sync(page)

        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

        pass_waf_challenge(page)
        time.sleep(random.uniform(3, 5))

        for idx, target in enumerate(SCRAPE_TARGETS):
            label = target["label"]
            stored_type = target["stored_type"]
            property_type = target["property_type"]
            base_url = target["url"]

            logger.info(f"\n--- {label} (max {MAX_LISTINGS_PER_TARGET}) ---")

            # Re-warm WAF between targets (not on first one)
            if idx > 0:
                logger.info("Re-warming WAF between targets...")
                try:
                    page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
                    time.sleep(random.uniform(5, 8))
                except Exception as e:
                    logger.warning(f"WAF re-warm failed: {e}")

            target_listings = []
            page_num = 1
            failures = 0

            while len(target_listings) < MAX_LISTINGS_PER_TARGET:
                if failures >= 3:
                    logger.error(f"3 failures for {label} — moving on")
                    break

                url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
                try:
                    logger.info(f"Page {page_num}: {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)

                    content = wait_for_page_content(page)

                    title = page.title()
                    logger.info(f"Loaded — title: '{title}', length: {len(content)}")

                    if len(content) < 5000:
                        logger.warning("Page too small — likely blocked by WAF")
                        failures += 1
                        # Try to re-pass WAF
                        logger.info("Attempting WAF re-pass...")
                        pass_waf_challenge(page)
                        time.sleep(random.uniform(3, 5))
                        continue

                    # Check for WAF challenge page
                    if "challenge" in content.lower()[:2000] or "just a moment" in content.lower()[:2000]:
                        logger.warning("WAF challenge page detected — waiting...")
                        time.sleep(15)
                        content = page.content()
                        if len(content) < 10000:
                            failures += 1
                            continue

                    page_listings = extract_listings(content, stored_type, property_type)

                    if not page_listings:
                        logger.warning(f"0 listings on page {page_num}")
                        failures += 1
                    else:
                        failures = 0
                        target_listings.extend(page_listings)
                        logger.info(f"Got {len(page_listings)} listings (total: {len(target_listings)})")

                        # Log first listing for verification
                        if page_num == 1 and page_listings:
                            first = page_listings[0]
                            logger.info(
                                f"Sample: ref={first['reference_no']}, "
                                f"community={first['community']}, "
                                f"building={first['building']}, "
                                f"beds={first['bedrooms']}, "
                                f"price={first['price']}, "
                                f"size={first['size_sqft']}, "
                                f"url={first['listing_url'][:60]}"
                            )

                        # Check for price changes before upserting
                        ref_nos = [l["reference_no"] for l in page_listings if l["reference_no"]]
                        current_prices = fetch_current_prices(ref_nos, stored_type)

                        changes = []
                        for l in page_listings:
                            ref = l["reference_no"]
                            if ref in current_prices and current_prices[ref] != l["price"] and current_prices[ref] > 0 and l["price"] > 0:
                                changes.append({
                                    "reference_no": ref,
                                    "listing_type": stored_type,
                                    "old_price": current_prices[ref],
                                    "new_price": l["price"],
                                })
                                logger.info(f"Price change: {ref} — AED {current_prices[ref]:,.0f} → AED {l['price']:,.0f}")

                        if changes:
                            logger.info(f"Detected {len(changes)} price changes!")
                            log_price_changes(changes)
                            total_price_changes += len(changes)

                        # Upsert after each page
                        logger.info(f"Upserting {len(page_listings)} listings...")
                        upsert_listings(page_listings)

                    if len(target_listings) >= MAX_LISTINGS_PER_TARGET:
                        target_listings = target_listings[:MAX_LISTINGS_PER_TARGET]
                        break

                    page_num += 1
                    time.sleep(random.uniform(3, 7))

                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}")
                    failures += 1
                    time.sleep(random.uniform(5, 10))
                    continue

            logger.info(f"✓ {label}: {len(target_listings)} listings scraped")
            all_listings.extend(target_listings)
            time.sleep(random.uniform(3, 7))

        browser.close()

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info(
        f"\n=== PF Scraper V2 finished ===\n"
        f"Start:    {start_time.isoformat()}\n"
        f"End:      {end_time.isoformat()}\n"
        f"Duration: {duration:.0f}s\n"
        f"Total listings scraped: {len(all_listings)}\n"
        f"Price changes detected: {total_price_changes}"
    )


if __name__ == "__main__":
    run_scraper()
