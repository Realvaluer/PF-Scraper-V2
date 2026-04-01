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

# Scrape targets: latest 50 sale + 50 rent for Dubai apartments and villas
# PF sorts by newest by default
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

MAX_LISTINGS_PER_TARGET = 50
UPSERT_BATCH_SIZE = 50

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def extract_from_next_data(page_content: str, stored_type: str, property_type: str) -> list[dict]:
    """Extract listing data from __NEXT_DATA__ JSON."""
    try:
        start = page_content.find('id="__NEXT_DATA__"')
        if start == -1:
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
        search_result = page_props.get("searchResult", {})
        properties = search_result.get("properties", [])

        if not properties:
            properties = page_props.get("properties", [])

        logger.info(f"Found {len(properties)} properties in __NEXT_DATA__")

        for prop in properties:
            try:
                reference_no = prop.get("referenceNumber", "")
                price = prop.get("price", 0)
                size_sqft = prop.get("area", 0)
                bedrooms_raw = prop.get("bedrooms", 0)
                building = prop.get("buildingName", "") or ""

                try:
                    price = float(price) if price else 0
                except (ValueError, TypeError):
                    price = 0
                try:
                    size_sqft = float(size_sqft) if size_sqft else 0
                except (ValueError, TypeError):
                    size_sqft = 0

                community_name = ""
                locations = prop.get("location", [])
                if isinstance(locations, list) and len(locations) >= 2:
                    community_name = locations[1].get("name", "") if isinstance(locations[1], dict) else ""
                elif isinstance(locations, list) and len(locations) >= 1:
                    community_name = locations[0].get("name", "") if isinstance(locations[0], dict) else ""

                try:
                    bed_num = int(bedrooms_raw)
                except (ValueError, TypeError):
                    bed_num = -1

                if bed_num == 0 or str(bedrooms_raw).lower() == "studio":
                    bedrooms = "Studio"
                elif bed_num >= 4:
                    bedrooms = "4+"
                elif bed_num > 0:
                    bedrooms = str(bed_num)
                else:
                    bedrooms = str(bedrooms_raw)

                price_per_sqft = round(price / size_sqft, 2) if size_sqft and price else 0

                listing_url = prop.get("url", "")
                if listing_url and not listing_url.startswith("http"):
                    listing_url = f"https://www.propertyfinder.ae{listing_url}"

                listings.append({
                    "reference_no": reference_no,
                    "listing_type": stored_type,
                    "property_type": property_type,
                    "community": community_name,
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
        logger.error(f"Failed to navigate __NEXT_DATA__: {e}")

    return listings


def wait_for_page_content(page) -> str:
    """Wait for actual page content to load."""
    time.sleep(3)

    try:
        page.wait_for_selector('script#__NEXT_DATA__', timeout=5000)
        logger.info("Found __NEXT_DATA__")
        return page.content()
    except Exception:
        pass

    try:
        page.wait_for_selector('script[type="application/ld+json"]', timeout=5000)
        logger.info("Found JSON-LD")
        return page.content()
    except Exception:
        pass

    try:
        page.wait_for_selector('[class*="property-card"], [class*="listing"], [data-testid*="property"]', timeout=10000)
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
    """Navigate to PF homepage and wait for WAF challenge to resolve."""
    logger.info("Navigating to PF homepage to pass WAF challenge...")
    try:
        page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(15)

        title = page.title()
        content = page.content()
        has_next_data = "__NEXT_DATA__" in content

        logger.info(f"Homepage title: {title}, length: {len(content)}, __NEXT_DATA__: {has_next_data}")

        if has_next_data or len(content) > 50000:
            logger.info("WAF challenge passed")
            return True

        logger.warning("WAF challenge may not have passed")
        return False
    except Exception as e:
        logger.error(f"Failed during WAF challenge: {e}")
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
                "--disable-extensions",
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

        # Step 1: Pass WAF challenge
        pass_waf_challenge(page)
        time.sleep(random.uniform(3, 5))

        # Step 2: Scrape each target
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
                    logger.info(f"Loaded — title: '{title}', length: {len(content)}")

                    if len(content) < 5000:
                        logger.warning("Page too small — blocked?")
                        failures += 1
                        time.sleep(random.uniform(5, 10))
                        continue

                    page_listings = extract_from_next_data(content, stored_type, property_type)

                    if not page_listings:
                        logger.warning(f"0 listings on page {page_num}")
                        failures += 1
                    else:
                        failures = 0
                        target_listings.extend(page_listings)
                        logger.info(f"Got {len(page_listings)} listings (total: {len(target_listings)})")

                        # Upsert every page so we don't lose data on crash
                        logger.info(f"Upserting {len(page_listings)} listings from page {page_num}...")
                        upsert_listings(page_listings)

                    # Stop if we have enough
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
