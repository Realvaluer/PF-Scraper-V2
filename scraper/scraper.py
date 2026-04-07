import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone

import httpx
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from .supabase_client import upsert_listings, fetch_current_prices, log_price_changes, sync_to_ddf, compute_dips_for_rows, backfill_dips, compute_txns_for_rows, backfill_txns, reset_txns, cleanup_duplicates, fetch_latest_listed_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MAX_PAGES_SAFETY_NET = 30
BACKFILL_DEFAULT_PAGES = 50

# ── Helper to generate targets ────────────────────────────────────────────────

PF_BASE = "https://www.propertyfinder.ae/en"

EMIRATES = [
    ("dubai", "Dubai"),
    ("abu-dhabi", "Abu Dhabi"),
    ("sharjah", "Sharjah"),
    ("ajman", "Ajman"),
    ("ras-al-khaimah", "Ras Al Khaimah"),
    ("fujairah", "Fujairah"),
    ("umm-al-quwain", "Umm Al Quwain"),
]

RESIDENTIAL_TYPES = [
    ("apartments", "apartment"),
    ("villas", "villa"),
    ("townhouses", "townhouse"),
    ("penthouses", "penthouse"),
]

COMMERCIAL_TYPES = [
    ("offices", "office"),
    ("warehouses", "warehouse"),
    ("shops", "shop"),
    ("showrooms", "showroom"),
    ("commercial-buildings", "commercial building"),
]

def _build_targets():
    targets = []
    for slug, city in EMIRATES:
        # Residential rent + sale
        for ptype_slug, ptype in RESIDENTIAL_TYPES:
            targets.append({"url": f"{PF_BASE}/rent/{slug}/{ptype_slug}-for-rent.html", "label": f"{city} {ptype.title()} (rent)", "stored_type": "rent", "property_type": ptype, "city": city, "category": "Residential"})
            targets.append({"url": f"{PF_BASE}/buy/{slug}/{ptype_slug}-for-sale.html", "label": f"{city} {ptype.title()} (sale)", "stored_type": "sale", "property_type": ptype, "city": city, "category": "Residential"})
        # Residential land (sale only)
        targets.append({"url": f"{PF_BASE}/buy/{slug}/land-for-sale.html", "label": f"{city} Land (sale)", "stored_type": "sale", "property_type": "land", "city": city, "category": "Residential"})
        # Commercial rent + sale (PF uses /commercial-rent/ and /commercial-buy/ paths)
        for ctype_slug, ctype in COMMERCIAL_TYPES:
            targets.append({"url": f"{PF_BASE}/commercial-rent/{slug}/{ctype_slug}-for-rent.html", "label": f"{city} {ctype.title()} (rent)", "stored_type": "rent", "property_type": ctype, "city": city, "category": "Commercial"})
            targets.append({"url": f"{PF_BASE}/commercial-buy/{slug}/{ctype_slug}-for-sale.html", "label": f"{city} {ctype.title()} (sale)", "stored_type": "sale", "property_type": ctype, "city": city, "category": "Commercial"})
        # Commercial land (sale only)
        targets.append({"url": f"{PF_BASE}/commercial-buy/{slug}/land-for-sale.html", "label": f"{city} Commercial Land (sale)", "stored_type": "sale", "property_type": "commercial land", "city": city, "category": "Commercial"})
    return targets

SCRAPE_TARGETS = _build_targets()

# ── Full Backfill Targets (bedroom + price splits to stay under 249 pages) ────

PF_RENT = "https://www.propertyfinder.ae/en/rent/dubai"
PF_SALE = "https://www.propertyfinder.ae/en/buy/dubai"

BACKFILL_BATCH_1 = [  # Apt Rent — ~11 targets
    {"url": f"{PF_RENT}/studio-apartments-for-rent.html", "label": "Rent Studio", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/1-bedroom-apartments-for-rent.html?price_to=60000", "label": "Rent 1BR <60K", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/1-bedroom-apartments-for-rent.html?price_from=60000&price_to=100000", "label": "Rent 1BR 60K-100K", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/1-bedroom-apartments-for-rent.html?price_from=100000", "label": "Rent 1BR >100K", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/2-bedroom-apartments-for-rent.html?price_to=80000", "label": "Rent 2BR <80K", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/2-bedroom-apartments-for-rent.html?price_from=80000&price_to=150000", "label": "Rent 2BR 80K-150K", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/2-bedroom-apartments-for-rent.html?price_from=150000", "label": "Rent 2BR >150K", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/3-bedroom-apartments-for-rent.html", "label": "Rent 3BR", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/4-bedroom-apartments-for-rent.html", "label": "Rent 4BR", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/5-bedroom-apartments-for-rent.html", "label": "Rent 5BR+", "stored_type": "rent", "property_type": "apartment"},
    {"url": f"{PF_RENT}/6-bedroom-apartments-for-rent.html", "label": "Rent 6BR+", "stored_type": "rent", "property_type": "apartment"},
]

BACKFILL_BATCH_2 = [  # Apt Sale — ~12 targets
    {"url": f"{PF_SALE}/studio-apartments-for-sale.html?price_to=500000", "label": "Sale Studio <500K", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/studio-apartments-for-sale.html?price_from=500000&price_to=1000000", "label": "Sale Studio 500K-1M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/studio-apartments-for-sale.html?price_from=1000000", "label": "Sale Studio >1M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/1-bedroom-apartments-for-sale.html?price_to=1000000", "label": "Sale 1BR <1M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/1-bedroom-apartments-for-sale.html?price_from=1000000&price_to=2000000", "label": "Sale 1BR 1M-2M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/1-bedroom-apartments-for-sale.html?price_from=2000000&price_to=4000000", "label": "Sale 1BR 2M-4M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/1-bedroom-apartments-for-sale.html?price_from=4000000", "label": "Sale 1BR >4M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/2-bedroom-apartments-for-sale.html?price_to=1500000", "label": "Sale 2BR <1.5M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/2-bedroom-apartments-for-sale.html?price_from=1500000&price_to=3000000", "label": "Sale 2BR 1.5M-3M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/2-bedroom-apartments-for-sale.html?price_from=3000000", "label": "Sale 2BR >3M", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/3-bedroom-apartments-for-sale.html", "label": "Sale 3BR", "stored_type": "sale", "property_type": "apartment"},
    {"url": f"{PF_SALE}/4-bedroom-apartments-for-sale.html", "label": "Sale 4BR+", "stored_type": "sale", "property_type": "apartment"},
]

BACKFILL_BATCH_3 = [  # Villas + Townhouses + Penthouses + Land — ~8 targets
    {"url": f"{PF_SALE}/villas-for-sale.html", "label": "Villa Sale", "stored_type": "sale", "property_type": "villa"},
    {"url": f"{PF_RENT}/villas-for-rent.html", "label": "Villa Rent", "stored_type": "rent", "property_type": "villa"},
    {"url": f"{PF_SALE}/townhouses-for-sale.html", "label": "Townhouse Sale", "stored_type": "sale", "property_type": "townhouse"},
    {"url": f"{PF_RENT}/townhouses-for-rent.html", "label": "Townhouse Rent", "stored_type": "rent", "property_type": "townhouse"},
    {"url": f"{PF_SALE}/penthouses-for-sale.html", "label": "Penthouse Sale", "stored_type": "sale", "property_type": "penthouse"},
    {"url": f"{PF_RENT}/penthouses-for-rent.html", "label": "Penthouse Rent", "stored_type": "rent", "property_type": "penthouse"},
    {"url": f"{PF_SALE}/land-for-sale.html", "label": "Land Sale", "stored_type": "sale", "property_type": "land"},
]

BACKFILL_BATCHES = {
    "1": BACKFILL_BATCH_1,
    "2": BACKFILL_BATCH_2,
    "3": BACKFILL_BATCH_3,
    "all": BACKFILL_BATCH_1 + BACKFILL_BATCH_2 + BACKFILL_BATCH_3,
}

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
            logger.info(f"First listing date fields: listed_date={fp.get('listed_date')}, "
                        f"last_refreshed_at={fp.get('last_refreshed_at')}, added_on={fp.get('added_on')}, "
                        f"created_at={fp.get('created_at')}, updated_at={fp.get('updated_at')}")

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

                # bathrooms
                bathrooms_raw = prop.get("bathrooms_value", 0) or prop.get("bathrooms", 0)
                try:
                    bathrooms = int(bathrooms_raw) if bathrooms_raw else 0
                except (ValueError, TypeError):
                    bathrooms = 0

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

                # completion_status → ready_off_plan
                completion_raw = prop.get("completion_status", "")
                if completion_raw in ("off_plan", "off_plan_primary"):
                    ready_off_plan = "Off-plan"
                elif completion_raw == "completed":
                    ready_off_plan = "Ready"
                else:
                    ready_off_plan = ""

                # furnished
                furnished_raw = prop.get("furnished", "")
                if furnished_raw == "YES":
                    furnished = "Furnished"
                elif furnished_raw == "PARTLY":
                    furnished = "Partly Furnished"
                elif furnished_raw == "NO":
                    furnished = "Unfurnished"
                else:
                    furnished = ""

                # Dates from PF
                listed_date = prop.get("listed_date", "") or prop.get("added_on", "") or ""
                last_refreshed_at = prop.get("last_refreshed_at", "") or ""

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
                    "bathrooms": bathrooms,
                    "size_sqft": size_sqft,
                    "price": price,
                    "price_per_sqft": price_per_sqft,
                    "listing_url": listing_url,
                    "ready_off_plan": ready_off_plan,
                    "furnished": furnished,
                    "listed_date": listed_date,
                    "last_refreshed_at": last_refreshed_at,
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


def _get_cutoff_for_target(target: dict) -> str | None:
    """Get the smart cutoff timestamp for a target (latest listed_date - 5 min).
    Returns ISO timestamp string, or None if no prior data exists (scrape all pages)."""
    purpose = "Rent" if target["stored_type"] == "rent" else "Sale"
    city = target.get("city", "Dubai")

    latest = fetch_latest_listed_date(purpose, city)
    if not latest:
        logger.info(f"No prior listed_date for {purpose}/{city} — will scrape up to page limit")
        return None

    try:
        # Parse ISO timestamp — handle both "Z" and "+00:00" formats
        ts = latest.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        cutoff = dt - timedelta(minutes=5)
        cutoff_str = cutoff.isoformat()
        logger.info(f"Smart cutoff for {purpose}/{city}: {cutoff_str} (latest={latest})")
        return cutoff_str
    except Exception as e:
        logger.warning(f"Failed to parse latest listed_date '{latest}': {e}")
        return None


def _all_listings_older_than_cutoff(page_listings: list[dict], cutoff: str) -> bool:
    """Check if ALL listings on a page have listed_date older than the cutoff."""
    if not page_listings:
        return False
    for l in page_listings:
        ld = l.get("listed_date", "")
        if not ld:
            # No date — can't determine, assume it's new
            return False
        try:
            ts = ld.replace("Z", "+00:00")
            if ts >= cutoff:
                return False  # At least one listing is newer than cutoff
        except Exception:
            return False
    return True


def send_resend_notification(
    total_scraped: int,
    new_ddf: int,
    dubai_sale: int,
    dubai_rent: int,
    total_dips: int,
    total_txns: int,
    price_changes: int,
    duration_s: float,
    target_count: int,
):
    """Send email notification via Resend after scrape completes."""
    resend_api_key = os.environ.get("RESEND_API_KEY", "")
    resend_to = os.environ.get("RESEND_TO", "")
    if not resend_api_key or not resend_to:
        logger.info("RESEND_API_KEY or RESEND_TO not set — skipping email notification")
        return

    mins = int(duration_s // 60)
    secs = int(duration_s % 60)

    subject = f"PF Scraper: {new_ddf} new listings ({dubai_sale} sale, {dubai_rent} rent)"

    html_body = f"""
    <h2>PF Scraper V2 — Run Complete</h2>
    <table style="border-collapse:collapse; font-family:Arial,sans-serif;">
      <tr><td style="padding:4px 12px;"><b>Targets scraped</b></td><td>{target_count}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Total listings scraped</b></td><td>{total_scraped:,}</td></tr>
      <tr><td style="padding:4px 12px;"><b>New DDF rows</b></td><td>{new_ddf:,}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Dubai Sale (new)</b></td><td>{dubai_sale:,}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Dubai Rent (new)</b></td><td>{dubai_rent:,}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Dips computed</b></td><td>{total_dips}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Txn comparisons</b></td><td>{total_txns}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Price changes</b></td><td>{price_changes}</td></tr>
      <tr><td style="padding:4px 12px;"><b>Duration</b></td><td>{mins}m {secs}s</td></tr>
    </table>
    """

    try:
        resp = httpx.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {resend_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": "PF Scraper <notifications@dxpdipfinder.com>",
                "to": [resend_to],
                "subject": subject,
                "html": html_body,
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            logger.info(f"Resend notification sent to {resend_to}")
        else:
            logger.warning(f"Resend failed: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"Resend notification failed: {e}")


def run_scraper(max_pages: int = None, property_types: list[str] = None, custom_targets: list[dict] = None, smart_stop: bool = True):
    pages = max_pages or MAX_PAGES_SAFETY_NET
    targets = custom_targets or SCRAPE_TARGETS
    if property_types and not custom_targets:
        targets = [t for t in targets if t["property_type"] in property_types]
    start_time = datetime.now(timezone.utc)
    logger.info(f"=== PF Scraper V2 started at {start_time.isoformat()} ({pages} pages, {len(targets)} targets, smart_stop={smart_stop}) ===")

    all_listings = []
    all_new_ddf_ids = []
    total_price_changes = 0
    # Track Dubai sale/rent counts for notification
    dubai_sale_new = 0
    dubai_rent_new = 0

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

        for idx, target in enumerate(targets):
            label = target["label"]
            stored_type = target["stored_type"]
            property_type = target["property_type"]
            base_url = target["url"]
            city = target.get("city", "Dubai")
            category = target.get("category", "Residential")

            # Smart stop: get cutoff timestamp for this target
            cutoff = None
            if smart_stop:
                cutoff = _get_cutoff_for_target(target)

            logger.info(f"\n--- [{idx+1}/{len(targets)}] {label} (max {pages} pages, cutoff={cutoff or 'none'}) ---")

            # Re-warm WAF between targets (not on first one)
            if idx > 0:
                logger.info("Re-warming WAF between targets...")
                try:
                    page.goto("https://www.propertyfinder.ae/en/", wait_until="domcontentloaded", timeout=30000)
                    time.sleep(random.uniform(5, 8))
                except Exception as e:
                    logger.warning(f"WAF re-warm failed: {e}")

            target_listings = []
            target_new_ids = []
            page_num = 1
            failures = 0

            while page_num <= pages:
                if failures >= 3:
                    logger.error(f"3 failures for {label} — moving on")
                    break

                if page_num == 1:
                    url = base_url
                elif "?" in base_url:
                    url = f"{base_url}&page={page_num}"
                else:
                    url = f"{base_url}?page={page_num}"
                try:
                    logger.info(f"Page {page_num}: {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)

                    content = wait_for_page_content(page)

                    title = page.title()
                    logger.info(f"Loaded — title: '{title}', length: {len(content)}")

                    if len(content) < 5000:
                        logger.warning("Page too small — likely blocked by WAF")
                        failures += 1
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

                    # Check for 404 page (title-based only — avoid false positives from JS bundles)
                    if "page not found" in title.lower() or "404" in title:
                        logger.info(f"404 for {label} — skipping target")
                        break

                    page_listings = extract_listings(content, stored_type, property_type)

                    if not page_listings:
                        logger.warning(f"0 listings on page {page_num}")
                        failures += 1
                    else:
                        failures = 0

                        # Inject city and category from target into each listing
                        for l in page_listings:
                            l["city"] = city
                            l["category"] = category

                        target_listings.extend(page_listings)
                        logger.info(f"Got {len(page_listings)} listings (total: {len(target_listings)})")

                        # Log first listing for verification
                        if page_num == 1 and page_listings:
                            first = page_listings[0]
                            logger.info(
                                f"Sample: ref={first['reference_no']}, "
                                f"city={city}, cat={category}, "
                                f"community={first['community']}, "
                                f"building={first['building']}, "
                                f"beds={first['bedrooms']}, "
                                f"price={first['price']}, "
                                f"size={first['size_sqft']}, "
                                f"listed_date={first.get('listed_date','')[:20]}"
                            )

                        # Smart stop: check if all listings on this page are older than cutoff
                        if cutoff and _all_listings_older_than_cutoff(page_listings, cutoff):
                            logger.info(f"All {len(page_listings)} listings older than cutoff — stopping {label}")
                            # Still process this last page before stopping
                            _process_page(page_listings, stored_type, target_new_ids)
                            total_price_changes += _detect_price_changes(page_listings, stored_type)
                            break

                        # Process page: upsert + sync
                        _process_page(page_listings, stored_type, target_new_ids)
                        total_price_changes += _detect_price_changes(page_listings, stored_type)

                    page_num += 1
                    time.sleep(random.uniform(3, 7))

                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}")
                    failures += 1
                    time.sleep(random.uniform(5, 10))
                    continue

            logger.info(f"✓ {label}: {len(target_listings)} listings scraped, {len(target_new_ids)} new DDF rows")
            all_listings.extend(target_listings)
            all_new_ddf_ids.extend(target_new_ids)

            # Track Dubai sale/rent for notification
            if city == "Dubai":
                if stored_type == "sale":
                    dubai_sale_new += len(target_new_ids)
                else:
                    dubai_rent_new += len(target_new_ids)

            time.sleep(random.uniform(3, 7))

        browser.close()

    # Compute dips for all newly inserted DDF rows
    total_dips = compute_dips_for_rows(all_new_ddf_ids)

    # Compute transaction comparisons for newly inserted DDF rows
    total_txns = compute_txns_for_rows(all_new_ddf_ids)

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info(
        f"\n=== PF Scraper V2 finished ===\n"
        f"Start:    {start_time.isoformat()}\n"
        f"End:      {end_time.isoformat()}\n"
        f"Duration: {duration:.0f}s\n"
        f"Total listings scraped: {len(all_listings)}\n"
        f"New DDF rows: {len(all_new_ddf_ids)}\n"
        f"Dubai Sale (new): {dubai_sale_new}\n"
        f"Dubai Rent (new): {dubai_rent_new}\n"
        f"Dips computed: {total_dips}\n"
        f"Txn comparisons computed: {total_txns}\n"
        f"Price changes detected: {total_price_changes}"
    )

    # Send Resend email notification
    send_resend_notification(
        total_scraped=len(all_listings),
        new_ddf=len(all_new_ddf_ids),
        dubai_sale=dubai_sale_new,
        dubai_rent=dubai_rent_new,
        total_dips=total_dips,
        total_txns=total_txns,
        price_changes=total_price_changes,
        duration_s=duration,
        target_count=len(targets),
    )


def _process_page(page_listings: list[dict], stored_type: str, new_ids_collector: list[int]):
    """Process a page of listings: add timestamp, upsert, sync to DDF."""
    now = datetime.now(timezone.utc).isoformat()
    for l in page_listings:
        l["scraped_at"] = now

    # Upsert to pf_listings_v2
    logger.info(f"Upserting {len(page_listings)} listings...")
    upsert_listings(page_listings)

    # Sync to ddf_listings
    new_ids = sync_to_ddf(page_listings)
    new_ids_collector.extend(new_ids)


def _detect_price_changes(page_listings: list[dict], stored_type: str) -> int:
    """Detect and log price changes for a page of listings. Returns count of changes."""
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

    return len(changes)


if __name__ == "__main__":
    import sys
    # Parse --property-type from any position
    pt_filter = None
    args = sys.argv[1:]
    if "--property-type" in args:
        pt_idx = args.index("--property-type")
        if pt_idx + 1 < len(args):
            pt_filter = [t.strip() for t in args[pt_idx + 1].split(",")]
            args = args[:pt_idx] + args[pt_idx + 2:]

    if args and args[0] == "--backfill-full":
        batch = args[1] if len(args) > 1 else "all"
        max_pg = int(args[2]) if len(args) > 2 else 249
        if batch not in BACKFILL_BATCHES:
            logger.error(f"Invalid batch: {batch}. Use 1, 2, 3, or all")
            sys.exit(1)
        targets = BACKFILL_BATCHES[batch]
        logger.info(f"=== FULL BACKFILL: batch={batch}, {len(targets)} targets, {max_pg} pages each ===")
        run_scraper(max_pages=max_pg, custom_targets=targets, smart_stop=False)
        logger.info("=== Backfill done. Running cleanup-duplicates ===")
        cleanup_duplicates()
    elif args and args[0] == "--backfill":
        pages = int(args[1]) if len(args) > 1 else BACKFILL_DEFAULT_PAGES
        logger.info(f"=== BACKFILL MODE: {pages} pages per target ===")
        run_scraper(max_pages=pages, property_types=pt_filter)
    elif args and args[0] == "--backfill-dips":
        backfill_dips()
    elif args and args[0] == "--reset-txns":
        limit = int(args[1]) if len(args) > 1 else 0
        row_ids = reset_txns(limit=limit)
        if row_ids:
            compute_txns_for_rows(row_ids)
        else:
            backfill_txns()
    elif args and args[0] == "--backfill-txns":
        backfill_txns()
    elif args and args[0] == "--cleanup-duplicates":
        cleanup_duplicates()
    else:
        run_scraper(property_types=pt_filter)
