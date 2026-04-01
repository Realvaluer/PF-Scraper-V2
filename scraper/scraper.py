import json
import logging
import random
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

COMMUNITIES = [
    "downtown-dubai",
    "dubai-marina",
    "business-bay",
    "jumeirah-village-circle-jvc",
    "palm-jumeirah",
]

COMMUNITY_LABELS = {
    "downtown-dubai": "Downtown Dubai",
    "dubai-marina": "Dubai Marina",
    "business-bay": "Business Bay",
    "jumeirah-village-circle-jvc": "Jumeirah Village Circle",
    "palm-jumeirah": "Palm Jumeirah",
}

LISTING_TYPES = ["buy", "rent"]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def build_url(community: str, listing_type: str, page: int) -> str:
    """Build PropertyFinder search URL."""
    # PF uses 'buy' in URL for sale listings
    base = f"https://www.propertyfinder.ae/en/{listing_type}/apartments-for-{listing_type}-in-{community}"
    if page > 1:
        return f"{base}/page-{page}"
    return base


def extract_listings_from_next_data(page_content: str, listing_type: str) -> list[dict]:
    """Extract listing data from __NEXT_DATA__ JSON."""
    try:
        # Find the __NEXT_DATA__ script content
        start = page_content.find('id="__NEXT_DATA__"')
        if start == -1:
            return []
        # Find the JSON start
        json_start = page_content.find(">", start) + 1
        json_end = page_content.find("</script>", json_start)
        raw_json = page_content[json_start:json_end]
        data = json.loads(raw_json)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to parse __NEXT_DATA__: {e}")
        return []

    listings = []
    try:
        # Navigate the Next.js data structure
        page_props = data.get("props", {}).get("pageProps", {})
        search_result = page_props.get("searchResult", {})
        properties = search_result.get("properties", [])

        for prop in properties:
            try:
                reference_no = prop.get("referenceNumber", "")
                price = prop.get("price", 0)
                size_sqft = prop.get("area", 0)
                bedrooms_raw = prop.get("bedrooms", 0)
                community = prop.get("location", [{}])
                building = prop.get("buildingName", "") or ""

                # Extract community name from location array
                community_name = ""
                locations = prop.get("location", [])
                if isinstance(locations, list) and len(locations) >= 2:
                    community_name = locations[1].get("name", "") if isinstance(locations[1], dict) else ""
                elif isinstance(locations, list) and len(locations) >= 1:
                    community_name = locations[0].get("name", "") if isinstance(locations[0], dict) else ""

                # Format bedrooms
                if bedrooms_raw == 0:
                    bedrooms = "Studio"
                elif bedrooms_raw >= 4:
                    bedrooms = "4+"
                else:
                    bedrooms = str(bedrooms_raw)

                # Calculate price per sqft
                price_per_sqft = round(price / size_sqft, 2) if size_sqft and price else 0

                # Build listing URL
                listing_url = prop.get("url", "")
                if listing_url and not listing_url.startswith("http"):
                    listing_url = f"https://www.propertyfinder.ae{listing_url}"

                # Map 'buy' back to 'sale' for storage
                stored_type = "sale" if listing_type == "buy" else "rent"

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
                logger.warning(f"Failed to parse individual listing: {e}")
                continue

    except Exception as e:
        logger.error(f"Failed to navigate __NEXT_DATA__ structure: {e}")

    return listings


def get_total_pages(page_content: str) -> int:
    """Extract total pages from __NEXT_DATA__."""
    try:
        start = page_content.find('id="__NEXT_DATA__"')
        if start == -1:
            return 0
        json_start = page_content.find(">", start) + 1
        json_end = page_content.find("</script>", json_start)
        raw_json = page_content[json_start:json_end]
        data = json.loads(raw_json)
        page_props = data.get("props", {}).get("pageProps", {})
        search_result = page_props.get("searchResult", {})
        return search_result.get("nbPages", 0)
    except Exception:
        return 0


def run_scraper():
    start_time = datetime.now(timezone.utc)
    logger.info(f"=== PF Scraper V2 started at {start_time.isoformat()} ===")

    all_listings = []
    consecutive_failures = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
        )
        page = context.new_page()
        stealth_sync(page)

        for community in COMMUNITIES:
            for listing_type in LISTING_TYPES:
                label = COMMUNITY_LABELS[community]
                stored_type = "sale" if listing_type == "buy" else "rent"
                logger.info(f"\n--- Scraping {label} ({stored_type}) ---")

                community_listings = []
                page_num = 1
                total_pages = None

                while True:
                    if consecutive_failures >= 3:
                        logger.error(
                            "3 consecutive failures — stopping. "
                            "PF may be blocking or returning challenge pages."
                        )
                        break

                    url = build_url(community, listing_type, page_num)
                    try:
                        logger.info(f"Page {page_num}: {url}")
                        page.goto(url, wait_until="networkidle", timeout=30000)

                        # Wait for __NEXT_DATA__ to confirm page loaded
                        try:
                            page.wait_for_selector(
                                'script#__NEXT_DATA__', timeout=15000
                            )
                        except Exception:
                            logger.warning(
                                f"__NEXT_DATA__ not found on page {page_num} — "
                                "possible challenge page"
                            )
                            consecutive_failures += 1
                            time.sleep(random.uniform(5, 10))
                            continue

                        content = page.content()

                        # Get total pages on first page
                        if total_pages is None:
                            total_pages = get_total_pages(content)
                            if total_pages == 0:
                                logger.warning(
                                    f"0 pages found for {label} ({stored_type}) — skipping"
                                )
                                break
                            logger.info(f"Total pages: {total_pages}")

                        page_listings = extract_listings_from_next_data(
                            content, listing_type
                        )

                        if not page_listings:
                            logger.warning(f"0 listings on page {page_num}")
                            consecutive_failures += 1
                        else:
                            consecutive_failures = 0
                            community_listings.extend(page_listings)

                        # Check if we've reached the last page
                        if page_num >= total_pages:
                            break

                        page_num += 1
                        # Random delay between pages
                        delay = random.uniform(3, 7)
                        logger.info(f"Waiting {delay:.1f}s before next page...")
                        time.sleep(delay)

                    except Exception as e:
                        logger.error(f"Error on page {page_num}: {e}")
                        consecutive_failures += 1
                        time.sleep(random.uniform(5, 10))
                        continue

                if consecutive_failures >= 3:
                    logger.error("Stopping scraper due to repeated failures")
                    break

                logger.info(
                    f"✓ {label} ({stored_type}): "
                    f"{page_num} pages, {len(community_listings)} listings"
                )
                all_listings.extend(community_listings)

                # Delay between community/type combos
                time.sleep(random.uniform(3, 7))

            if consecutive_failures >= 3:
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
