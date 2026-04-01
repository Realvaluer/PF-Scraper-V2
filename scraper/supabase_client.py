import os
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def upsert_listings(listings: list[dict]) -> None:
    """Upsert listings into pf_listings_v2 in batches of 50."""
    if not listings:
        logger.info("No listings to upsert")
        return

    total_upserted = 0
    for i in range(0, len(listings), 50):
        batch = listings[i : i + 50]
        result = (
            supabase.table("pf_listings_v2")
            .upsert(batch, on_conflict="reference_no,listing_type")
            .execute()
        )
        count = len(result.data) if result.data else 0
        total_upserted += count
        logger.info(f"Batch {i // 100 + 1}: upserted {count} rows")

    logger.info(f"Total upserted: {total_upserted} rows")
