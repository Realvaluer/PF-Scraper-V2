import os
import re
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def sanitize_listings(listings: list[dict]) -> list[dict]:
    """Remove non-printable characters from all string fields."""
    cleaned = []
    for listing in listings:
        clean = {}
        for key, value in listing.items():
            if isinstance(value, str):
                # Remove non-printable ASCII characters
                clean[key] = re.sub(r'[^\x20-\x7E]', '', value)
            else:
                clean[key] = value
        cleaned.append(clean)
    return cleaned


def upsert_listings(listings: list[dict]) -> None:
    """Upsert listings into pf_listings_v2 in batches of 50."""
    if not listings:
        logger.info("No listings to upsert")
        return

    listings = sanitize_listings(listings)

    total_upserted = 0
    for i in range(0, len(listings), 50):
        batch = listings[i : i + 50]
        try:
            result = (
                supabase.table("pf_listings_v2")
                .upsert(batch, on_conflict="reference_no,listing_type")
                .execute()
            )
            count = len(result.data) if result.data else 0
            total_upserted += count
            logger.info(f"Batch {i // 50 + 1}: upserted {count} rows")
        except Exception as e:
            logger.error(f"Batch {i // 50 + 1} failed: {e}")

    logger.info(f"Total upserted: {total_upserted} rows")
