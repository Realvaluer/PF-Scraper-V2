import json
import logging
import random
import re
import time
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from supabase_client import upsert_listings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SCRAPE_TARGETS = [
    {
        "url": "https://www.propertyfinder.ae/en/buy/dubai/apartments-for-sale.html",
        "label": "Dubai Apartments (sale)",
        "stored_type": "sale",
        "property_type": "apartment",
    },
    {
        "url": "https://www.propertyfinder.ae/en/rent/dubai/apartments-for-rent.html",
        "label": "Dubai Apartments (rent)",
        "stored_type": "rent",
        "property_type": "apartment",
    },
]

MAX_LISTINGS_PER_TARGET = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def extract_from_json_ld(page_content: str, stored_type: str, property_type: str) -> list[dict]:
    """Extract listing data from JSON-LD structured data (primary method)."""
    listings = []
    try:
        pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(pattern, page_content, re.DOTALL)

        for match in matches:
            try:
                data = json.loads(match)
            except json.JSONDecodeError:
                continue

            # Find itemListElement
            item_list = None
            if isinstance(data, dict):
                if "mainEntity" in data:
                    main = data["mainEntity"]
                    if isinstance(main, dict) and "itemListElement" in main:
                        item_list = main["itemListElement"]
                if not item_list and "itemListElement" in data:
                    item_list = data["itemListElement"]

            if not item_list:
                continue

            logger.info(f"Found {len(item_list)} items in JSON-LD")

            for item in item_list:
                try:
                    # item can be {item: {...}} or direct property
                    prop = item.get("item", item) if isinstance(item, dict) else item

                    # Reference number from @id
                    ref_no = str(prop.get("@id", ""))

                    # URL
                    url = prop.get("url", "")
                    if url and not url.startswith("http"):
                        url = f"https://www.propertyfinder.ae{url}"

                    # Price from offers
                    price = 0
                    offers = prop.get("offers", [])
                    if isinstance(offers, list) and offers:
                        offer = offers[0] if isinstance(offers[0], dict) else {}
                        price_spec = offer.get("priceSpecification", {})
                        if isinstance(price_spec, dict):
                            price = price_spec.get("price", 0)
                        if not price:
                            price = offer.get("price", 0)
                    elif isinstance(offers, dict):
                        price_spec = offers.get("priceSpecification", {})
                        if isinstance(price_spec, dict):
                            price = price_spec.get("price", 0)
                        if not price:
                            price = offers.get("price", 0)
                    try:
                        price = float(str(price).replace(",", "")) if price else 0
                    except (ValueError, TypeError):
                        price = 0

                    # Size from floorSize
                    size_sqft = 0
                    floor_size = prop.get("floorSize", {})
                    if isinstance(floor_size, dict):
                        size_sqft = floor_size.get("value", 0)
                    try:
                        size_sqft = float(str(size_sqft).replace(",", "")) if size_sqft else 0
                    except (ValueError, TypeError):
                        size_sqft = 0

                    # Community from address
                    community_name = ""
                    address = prop.get("address", {})
                    if isinstance(address, dict):
                        community_name = address.get("addressLocality", "")
                        if not community_name:
                            community_name = address.get("name", "")

                    # Name (contains bedrooms + building info)
                    name = prop.get("name", "")

                    # Parse bedrooms from name
                    bedrooms = ""
                    if "studio" in name.lower():
                        bedrooms = "Studio"
                    else:
                        bed_match = re.search(r"(\d+)\s*(?:bed|br|bedroom)", name, re.IGNORECASE)
                        if bed_match:
                            bed_num = int(bed_match.group(1))
                            bedrooms = "4+" if bed_num >= 4 else str(bed_num)

                    # Building from name — often after "in" or "|"
                    building = ""
                    # Try "in BuildingName" pattern
                    in_match = re.search(r"\bin\s+([A-Z][^|,]+?)(?:\s*[|,]|\s*$)", name)
                    if in_match:
                        building = in_match.group(1).strip()

                    # Price per sqft
                    price_per_sqft = round(price / size_sqft, 2) if size_sqft and price else 0

                    listings.append({
                        "reference_no": ref_no,
                        "listing_type": stored_type,
                        "property_type": property_type,
                        "community": community_name,
                        "building": building,
                        "bedrooms": bedrooms,
                        "size_sqft": size_sqft,
                        "price": price,
                        "price_per_sqft": price_per_sqft,
                        "listing_url": url,
                    })
                except Exception as e:
                    logger.warning(f"Failed to parse JSON-LD listing: {e}")
                    continue

    except Exception as e:
        logger.error(f"Failed to extract JSON-LD: {e}")

    return listings


def debug_next_data(page_content: str):
    """Log the actual __NEXT_DATA__ property structure for debugging."""
    try:
        start = page_content.find('id="__NEXT_DATA__"')
        if start == -1:
            return
        json_start = page_content.find(">", start) + 1
        json_end = page_content.find("</script>", json_start)
        raw_json = page_content[json_start:json_end]
        data = json.loads(raw_json)

        page_props = data.get("props", {}).get("pageProps", {})

        # Log top-level keys
        logger.info(f"__NEXT_DATA__ pageProps keys: {list(page_props.keys())}")

        # Try to find properties
        search_result = page_props.get("searchResult", {})
        if search_result:
            logger.info(f"searchResult keys: {list(search_result.keys())}")
            properties = search_result.get("properties", [])
            if properties and len(properties) > 0:
                first = properties[0]
                logger.info(f"First property keys: {list(first.keys())}")
                # Log a few key fields
                for key in ["referenceNumber", "reference", "ref", "id", "price", "area", "size",
                            "bedrooms", "beds", "bedroom", "buildingName", "building",
                            "location", "community", "url", "link", "slug"]:
                    if key in first:
                        val = first[key]
                        if isinstance(val, (dict, list)):
                            logger.info(f"  {key}: {json.dumps(val)[:200]}")
                        else:
                            logger.info(f"  {key}: {val}")
    except Exception as e:
        logger.error(f"Debug __NEXT_DATA__ failed: {e}")


def wait_for_page_content(page) -> str:
    """Wait for page content to load."""
    time.sleep(3)

    try:
        page.wait_for_selector('script[type="application/ld+json"]', timeout=10000)
        logger.info("Found JSON-LD script tag")
        return page.content()
    except Exception:
        pass

    try:
        page.wait_for_selector('script#__NEXT_DATA__', timeout=5000)
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
        has_ld = "application/ld+json" in content

        logger.info(f"Homepage length: {len(content)}, JSON-LD: {has_ld}")

        if has_ld or len(content) > 50000:
            logger.info("WAF challenge passed")
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

        for target in SCRAPE_TARGETS:
            label = target["label"]
            stored_type = target["stored_type"]
            property_type = target["property_type"]
            base_url = target["url"]

            logger.info(f"\n--- {label} (max {MAX_LISTINGS_PER_TARGET}) ---")

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
                    has_ld = "application/ld+json" in content
                    has_next = "__NEXT_DATA__" in content
                    logger.info(f"Loaded — title: '{title}', length: {len(content)}, JSON-LD: {has_ld}, __NEXT_DATA__: {has_next}")

                    if len(content) < 5000:
                        logger.warning("Page too small — blocked?")
                        failures += 1
                        time.sleep(random.uniform(5, 10))
                        continue

                    # Debug: log __NEXT_DATA__ structure on first page
                    if page_num == 1:
                        debug_next_data(content)

                    # Extract using JSON-LD (primary method)
                    page_listings = extract_from_json_ld(content, stored_type, property_type)

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
                            logger.info(f"Sample listing: ref={first['reference_no']}, "
                                       f"community={first['community']}, "
                                       f"price={first['price']}, "
                                       f"size={first['size_sqft']}, "
                                       f"beds={first['bedrooms']}")

                        # Upsert after each page
                        logger.info(f"Upserting {len(page_listings)} listings from page {page_num}...")
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

            logger.info(f"✓ {label}: {len(target_listings)} listings scraped and upserted")
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
        f"Total listings scraped: {len(all_listings)}"
    )


if __name__ == "__main__":
    run_scraper()
