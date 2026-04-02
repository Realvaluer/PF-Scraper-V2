import os
import re
import json
import logging
import httpx

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"].strip()

REST_URL = f"{SUPABASE_URL}/rest/v1/pf_listings_v2"
PRICE_HISTORY_URL = f"{SUPABASE_URL}/rest/v1/pf_price_history"

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

# Read-only headers (no Prefer needed for GET)
READ_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

# Insert-only headers (no merge-duplicates needed for price history)
INSERT_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


def sanitize_listings(listings: list[dict]) -> list[dict]:
    """Remove non-printable characters from all string fields."""
    cleaned = []
    for listing in listings:
        clean = {}
        for key, value in listing.items():
            if isinstance(value, str):
                clean[key] = re.sub(r'[^\x20-\x7E]', '', value)
            else:
                clean[key] = value
        cleaned.append(clean)
    return cleaned


def fetch_current_prices(reference_nos: list[str], listing_type: str) -> dict[str, float]:
    """Fetch current prices for given reference numbers from Supabase.
    Returns dict mapping reference_no -> current price."""
    if not reference_nos:
        return {}

    prices = {}
    try:
        # Build comma-separated list for PostgREST in filter
        refs_csv = ",".join(f'"{r}"' for r in reference_nos)
        response = httpx.get(
            REST_URL,
            headers=READ_HEADERS,
            params={
                "select": "reference_no,price",
                "listing_type": f"eq.{listing_type}",
                "reference_no": f"in.({refs_csv})",
            },
            timeout=15,
        )
        if response.status_code == 200:
            rows = response.json()
            for row in rows:
                ref = row.get("reference_no", "")
                price = row.get("price", 0)
                if ref and price:
                    prices[ref] = float(price)
            logger.info(f"Fetched {len(prices)} existing prices for comparison")
        else:
            logger.warning(f"Failed to fetch prices: {response.status_code} — {response.text[:200]}")
    except Exception as e:
        logger.warning(f"Failed to fetch prices: {e}")

    return prices


def log_price_changes(changes: list[dict]) -> None:
    """Insert price change records into pf_price_history."""
    if not changes:
        return

    try:
        response = httpx.post(
            PRICE_HISTORY_URL,
            headers=INSERT_HEADERS,
            json=changes,
            timeout=15,
        )
        if response.status_code in (200, 201):
            logger.info(f"Logged {len(changes)} price changes to history")
        else:
            logger.error(f"Price history insert failed: {response.status_code} — {response.text[:200]}")
    except Exception as e:
        logger.error(f"Price history insert failed: {e}")


def upsert_listings(listings: list[dict]) -> None:
    """Upsert listings into pf_listings_v2 via REST API."""
    if not listings:
        logger.info("No listings to upsert")
        return

    listings = sanitize_listings(listings)

    # Deduplicate by (reference_no, listing_type) — keep last occurrence
    seen = {}
    for listing in listings:
        key = (listing.get("reference_no", ""), listing.get("listing_type", ""))
        seen[key] = listing
    listings = list(seen.values())
    logger.info(f"After dedup: {len(listings)} unique listings")

    total_upserted = 0
    for i in range(0, len(listings), 50):
        batch = listings[i : i + 50]
        try:
            response = httpx.post(
                REST_URL,
                headers=HEADERS,
                json=batch,
                params={"on_conflict": "reference_no,listing_type"},
                timeout=30,
            )
            if response.status_code in (200, 201):
                count = len(response.json()) if response.text else len(batch)
                total_upserted += count
                logger.info(f"Batch {i // 50 + 1}: upserted {count} rows")
            else:
                logger.error(
                    f"Batch {i // 50 + 1} failed: {response.status_code} — {response.text[:200]}"
                )
        except Exception as e:
            logger.error(f"Batch {i // 50 + 1} failed: {e}")

    logger.info(f"Total upserted: {total_upserted} rows")
