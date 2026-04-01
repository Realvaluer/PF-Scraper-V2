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

# (url_prefix, url_word, stored_type)
LISTING_TYPES = [
    ("buy", "sale", "sale"),
    ("rent", "rent", "rent"),
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def build_url(community: str, url_prefix: str, url_word: str, page: int) -> str:
    """Build PropertyFinder search URL.

    Sale: /en/buy/apartments-for-sale-in-downtown-dubai
    Rent: /en/rent/apartments-for-rent-in-downtown-dubai
    """
    base = f"https://www.propertyfinder.ae/en/{url_prefix}/apartments-for-{url_word}-in-{community}"
    if page > 1:
        return f"{base}/page-{page}"
    return base


def extract_listings_from_next_data(page_content: str, stored_type: str) -> list[dict]:
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

        for prop in properties:
            try:
                reference_no = prop.get("referenceNumber", "")
                price = prop.get("price", 0)
                size_sqft = prop.get("area", 0)
                bedrooms_raw = prop.get("bedrooms", 0)
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


def pass_waf_challenge(page) -> bool:
    """Navigate to PF homepage and wait for WAF challenge to resolve."""
    logger.info("Navigating to PF homepage to pass WAF challenge...")
    try:
        page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
        # Wait for the challenge to resolve — WAF sets cookies after JS executes
        time.sleep(10)
        # Try waiting for any real page content to appear
        try:
            page.wait_for_selector('a[href*="propertyfinder"]', timeout=20000)
            logger.info("WAF challenge passed — homepage loaded")
            return True
        except Exception:
            # Check if __NEXT_DATA__ is present (another sign of success)
            content = page.content()
            if "__NEXT_DATA__" in content:
                logger.info("WAF challenge passed — __NEXT_DATA__ found")
                return True
            # Last resort: check page title
            title = page.title()
            logger.info(f"Page title after challenge wait: {title}")
            if "propertyfinder" in title.lower() or "property" in title.lower():
                logger.info("WAF challenge likely passed based on title")
                return True
            logger.warning("WAF challenge may not have been passed")
            # Log a snippet of the page to diagnose
            snippet = content[:500]
            logger.info(f"Page snippet: {snippet}")
            return False
    except Exception as e:
        logger.error(f"Failed during WAF challenge: {e}")
        return False


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

        # Remove webdriver flag
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

        # Step 1: Pass WAF challenge on homepage first
        waf_passed = pass_waf_challenge(page)
        if not waf_passed:
            logger.warning("WAF challenge may not have passed — continuing anyway")

        time.sleep(random.uniform(3, 5))

        # Step 2: Scrape each community
        for community in COMMUNITIES:
            for url_prefix, url_word, stored_type in LISTING_TYPES:
                label = COMMUNITY_LABELS[community]
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

                    url = build_url(community, url_prefix, url_word, page_num)
                    try:
                        logger.info(f"Page {page_num}: {url}")
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)

                        # Wait for page to fully render
                        time.sleep(random.uniform(3, 5))

                        # Wait for __NEXT_DATA__ to confirm page loaded
                        try:
                            page.wait_for_selector(
                                'script#__NEXT_DATA__', timeout=20000
                            )
                        except Exception:
                            # Log what we see for debugging
                            title = page.title()
                            logger.warning(
                                f"__NEXT_DATA__ not found on page {page_num} — "
                                f"title: '{title}' — possible challenge page"
                            )
                            consecutive_failures += 1
                            time.sleep(random.uniform(5, 10))
                            continue

                        content = page.content()
                        consecutive_failures = 0

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
                            content, stored_type
                        )

                        if not page_listings:
                            logger.warning(f"0 listings on page {page_num}")
                        else:
                            community_listings.extend(page_listings)

                        # Check if we've reached the last page
                        if page_num >= total_pages:
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
