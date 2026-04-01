import os
import re
import json
import logging
import httpx

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"].strip()

REST_URL = f"{SUPABASE_URL}/rest/v1/pf_listings_v2"
HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
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


def upsert_listings(listings: list[dict]) -> None:
    """Upsert listings into pf_listings_v2 via REST API."""
    if not listings:
        logger.info("No listings to upsert")
        return

    listings = sanitize_listings(listings)

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
