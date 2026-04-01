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

# (url_slug, display_label)
COMMUNITIES = [
    ("downtown-dubai", "Downtown Dubai"),
    ("dubai-marina", "Dubai Marina"),
    ("business-bay", "Business Bay"),
    ("jumeirah-village-circle", "Jumeirah Village Circle"),
    ("palm-jumeirah", "Palm Jumeirah"),
]

# (url_prefix, url_word, stored_type)
LISTING_TYPES = [
    ("buy", "sale", "sale"),
    ("rent", "rent", "rent"),
]

# Max pages per community/type combo (3 pages ≈ 50 listings for testing)
MAX_PAGES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def build_url(community_slug: str, url_prefix: str, url_word: str, page: int) -> str:
    """Build PropertyFinder search URL."""
    base = f"https://www.propertyfinder.ae/en/{url_prefix}/dubai/apartments-for-{url_word}-{community_slug}.html"
    if page > 1:
        return f"{base}?page={page}"
    return base


def extract_from_json_ld(page_content: str, stored_type: str) -> list[dict]:
    """Extract listing data from JSON-LD structured data."""
    listings = []
    try:
        # Find all JSON-LD script tags
        pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(pattern, page_content, re.DOTALL)

        for match in matches:
            try:
                data = json.loads(match)
            except json.JSONDecodeError:
                continue

            # Look for SearchResultsPage or ItemList with listings
            item_list = None
            if isinstance(data, dict):
                if "mainEntity" in data and "itemListElement" in data.get("mainEntity", {}):
                    item_list = data["mainEntity"]["itemListElement"]
                elif "itemListElement" in data:
                    item_list = data["itemListElement"]

            if not item_list:
                continue

            logger.info(f"Found {len(item_list)} items in JSON-LD")

            for item in item_list:
                try:
                    prop = item.get("item", item)

                    # Extract reference number from @id or url
                    ref_no = str(prop.get("@id", ""))
                    url = prop.get("url", "")
                    if url and not url.startswith("http"):
                        url = f"https://www.propertyfinder.ae{url}"

                    # Extract price
                    price = 0
                    offers = prop.get("offers", {})
                    if isinstance(offers, dict):
                        price_spec = offers.get("priceSpecification", {})
                        if isinstance(price_spec, dict):
                            price = price_spec.get("price", 0)
                        if not price:
                            price = offers.get("price", 0)
                    if isinstance(price, str):
                        price = float(price.replace(",", "")) if price else 0

                    # Extract size
                    size_sqft = 0
                    floor_size = prop.get("floorSize", {})
                    if isinstance(floor_size, dict):
                        size_sqft = floor_size.get("value", 0)
                    if isinstance(size_sqft, str):
                        size_sqft = float(size_sqft.replace(",", "")) if size_sqft else 0

                    # Extract name for bedrooms info
                    name = prop.get("name", "")

                    # Parse bedrooms from name (e.g., "2 Bedroom Apartment...")
                    bedrooms = ""
                    if "studio" in name.lower():
                        bedrooms = "Studio"
                    else:
                        bed_match = re.search(r"(\d+)\s*(?:bed|br)", name, re.IGNORECASE)
                        if bed_match:
                            bed_num = int(bed_match.group(1))
                            bedrooms = "4+" if bed_num >= 4 else str(bed_num)

                    # Extract community from address
                    community_name = ""
                    address = prop.get("address", {})
                    if isinstance(address, dict):
                        community_name = address.get("addressLocality", "")

                    # Extract building from name
                    building = ""
                    # Building name is often after "in" in the listing name
                    in_match = re.search(r"\bin\s+(.+?)(?:\s*,|\s*$)", name)
                    if in_match:
                        building = in_match.group(1).strip()

                    # Calculate price per sqft
                    price_per_sqft = round(price / size_sqft, 2) if size_sqft and price else 0

                    listings.append({
                        "reference_no": ref_no,
                        "listing_type": stored_type,
                        "property_type": "apartment",
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
        logger.error(f"Failed to extract JSON-LD data: {e}")

    return listings


def extract_from_next_data(page_content: str, stored_type: str) -> list[dict]:
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

                # Ensure numeric types
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

                # Handle bedrooms — can be int, str, or None
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
                    "property_type": "apartment",
                    "community": community_name,
                    "building": building,
                    "bedrooms": bedrooms,
                    "size_sqft": size_sqft,
                    "price": price,
                    "price_per_sqft": price_per_sqft,
                    "listing_url": listing_url,
                })
            except Exception as e:
                logger.warning(f"Failed to parse __NEXT_DATA__ listing: {e}")
                continue

    except Exception as e:
        logger.error(f"Failed to navigate __NEXT_DATA__ structure: {e}")

    return listings


def extract_listings(page_content: str, stored_type: str) -> list[dict]:
    """Try multiple extraction methods."""
    # Method 1: __NEXT_DATA__
    listings = extract_from_next_data(page_content, stored_type)
    if listings:
        logger.info(f"Extracted {len(listings)} listings via __NEXT_DATA__")
        return listings

    # Method 2: JSON-LD
    listings = extract_from_json_ld(page_content, stored_type)
    if listings:
        logger.info(f"Extracted {len(listings)} listings via JSON-LD")
        return listings

    logger.warning("No listings extracted from any method")
    return []


def get_total_pages(page_content: str) -> int:
    """Extract total pages from page content."""
    # Try __NEXT_DATA__ first
    try:
        start = page_content.find('id="__NEXT_DATA__"')
        if start != -1:
            json_start = page_content.find(">", start) + 1
            json_end = page_content.find("</script>", json_start)
            raw_json = page_content[json_start:json_end]
            data = json.loads(raw_json)
            page_props = data.get("props", {}).get("pageProps", {})
            search_result = page_props.get("searchResult", {})
            nb_pages = search_result.get("nbPages", 0)
            if nb_pages:
                return nb_pages
    except Exception:
        pass

    # Try to find pagination info from JSON-LD numberOfItems
    try:
        pattern = r'"numberOfItems"\s*:\s*(\d+)'
        match = re.search(pattern, page_content)
        if match:
            total_items = int(match.group(1))
            # PF shows ~25 per page
            pages = (total_items // 25) + (1 if total_items % 25 else 0)
            logger.info(f"Estimated {pages} pages from {total_items} total items")
            return min(pages, 100)  # Cap at 100 pages to be safe
    except Exception:
        pass

    return 0


def pass_waf_challenge(page) -> bool:
    """Navigate to PF homepage and wait for WAF challenge to resolve."""
    logger.info("Navigating to PF homepage to pass WAF challenge...")
    try:
        page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
        # Wait generously for WAF challenge
        time.sleep(15)

        # Check if page loaded
        title = page.title()
        logger.info(f"Homepage title: {title}")

        content = page.content()
        has_next_data = "__NEXT_DATA__" in content
        has_json_ld = "application/ld+json" in content
        content_length = len(content)

        logger.info(
            f"Homepage check — length: {content_length}, "
            f"__NEXT_DATA__: {has_next_data}, JSON-LD: {has_json_ld}"
        )

        if has_next_data or has_json_ld or content_length > 50000:
            logger.info("WAF challenge passed — real content detected")
            return True

        logger.warning("WAF challenge may not have passed — small content or no data tags")
        logger.info(f"Page snippet: {content[:500]}")
        return False
    except Exception as e:
        logger.error(f"Failed during WAF challenge: {e}")
        return False


def wait_for_page_content(page, timeout: int = 30) -> str:
    """Wait for actual page content to load, trying multiple signals."""
    # First wait a bit for initial render
    time.sleep(3)

    # Try waiting for __NEXT_DATA__
    try:
        page.wait_for_selector('script#__NEXT_DATA__', timeout=5000)
        logger.info("Found __NEXT_DATA__ script tag")
        return page.content()
    except Exception:
        pass

    # Try waiting for JSON-LD
    try:
        page.wait_for_selector('script[type="application/ld+json"]', timeout=5000)
        logger.info("Found JSON-LD script tag")
        return page.content()
    except Exception:
        pass

    # Try waiting for listing cards in the DOM
    try:
        page.wait_for_selector('[class*="property-card"], [class*="listing"], [data-testid*="property"]', timeout=10000)
        logger.info("Found property card elements")
        return page.content()
    except Exception:
        pass

    # Last resort — wait for networkidle and return whatever we have
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    content = page.content()
    logger.info(f"Page content length after all waits: {len(content)}")
    return content


def run_scraper():
    start_time = datetime.now(timezone.utc)
    logger.info(f"=== PF Scraper V2 started at {start_time.isoformat()} ===")

    all_listings = []
    consecutive_failures = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
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
        waf_passed = pass_waf_challenge(page)
        if not waf_passed:
            logger.warning("WAF challenge may not have passed — continuing anyway")

        time.sleep(random.uniform(3, 5))

        # Step 2: Scrape each community
        for community_slug, community_label in COMMUNITIES:
            for url_prefix, url_word, stored_type in LISTING_TYPES:
                logger.info(f"\n--- Scraping {community_label} ({stored_type}) ---")

                community_listings = []
                page_num = 1
                total_pages = None

                while True:
                    if consecutive_failures >= 5:
                        logger.error("5 consecutive failures — stopping.")
                        break

                    url = build_url(community_slug, url_prefix, url_word, page_num)
                    try:
                        logger.info(f"Page {page_num}: {url}")
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)

                        content = wait_for_page_content(page)

                        # Debug: log what we found
                        has_next = "__NEXT_DATA__" in content
                        has_ld = "application/ld+json" in content
                        title = page.title()
                        logger.info(
                            f"Page loaded — title: '{title}', "
                            f"length: {len(content)}, "
                            f"__NEXT_DATA__: {has_next}, JSON-LD: {has_ld}"
                        )

                        if len(content) < 5000:
                            logger.warning("Page content too small — likely blocked")
                            consecutive_failures += 1
                            time.sleep(random.uniform(5, 10))
                            continue

                        # Get total pages on first page
                        if total_pages is None:
                            total_pages = get_total_pages(content)
                            if total_pages == 0:
                                logger.warning(
                                    f"Could not determine pages for {community_label} ({stored_type})"
                                )
                                # Try with just 1 page
                                total_pages = 1

                            logger.info(f"Total pages: {total_pages}")

                        # Extract listings
                        page_listings = extract_listings(content, stored_type)

                        if not page_listings:
                            logger.warning(f"0 listings parsed on page {page_num}")
                            consecutive_failures += 1
                        else:
                            consecutive_failures = 0
                            community_listings.extend(page_listings)
                            logger.info(f"Parsed {len(page_listings)} listings from page {page_num}")

                        # Check if we've reached the last page
                        if page_num >= min(total_pages, MAX_PAGES):
                            break

                        page_num += 1
                        delay = random.uniform(3, 7)
                        logger.info(f"Waiting {delay:.1f}s before next page...")
                        time.sleep(delay)

                    except Exception as e:
                        logger.error(f"Error on page {page_num}: {e}")
                        consecutive_failures += 1
                        time.sleep(random.uniform(5, 10))
                        continue

                if consecutive_failures >= 5:
                    logger.error("Stopping scraper due to repeated failures")
                    break

                logger.info(
                    f"✓ {community_label} ({stored_type}): "
                    f"{page_num} pages, {len(community_listings)} listings"
                )
                all_listings.extend(community_listings)
                # Reset consecutive failures between combos
                consecutive_failures = 0

                time.sleep(random.uniform(3, 7))

            if consecutive_failures >= 5:
                break

        browser.close()

    # Upsert to Supabase
    logger.info(f"\nUpserting {len(all_listings)} listings to Supabase...")
    upsert_listings(all_listings)

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
