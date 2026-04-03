import os
import re
import json
import hashlib
import logging
import httpx

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"].strip()

REST_URL = f"{SUPABASE_URL}/rest/v1/pf_listings_v2"
PRICE_HISTORY_URL = f"{SUPABASE_URL}/rest/v1/pf_price_history"
DDF_URL = f"{SUPABASE_URL}/rest/v1/ddf_listings"

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

# DDF insert headers — ON CONFLICT (dup_hash) DO NOTHING, return inserted rows
DDF_INSERT_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=ignore-duplicates,return=representation",
}

# DDF update headers
DDF_UPDATE_HEADERS = {
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

    # Remove fields not in pf_listings_v2 schema
    PF_EXCLUDE = {"bathrooms", "scraped_at"}
    listings = [{k: v for k, v in l.items() if k not in PF_EXCLUDE} for l in listings]
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


# ── DDF Sync ──────────────────────────────────────────────────────────────────


def compute_dup_hash(ref: str, source: str, price, url: str = "") -> str:
    """MD5 hash matching V6 format: ref|source|price or source|url."""
    if ref:
        raw = f"{ref}|{source}|{price}"
    else:
        raw = f"{source}|{url}"
    return hashlib.md5(raw.encode()).hexdigest()


def fetch_ddf_latest_prices(reference_nos: list[str]) -> dict[str, float]:
    """Fetch latest price_aed per reference_no from ddf_listings."""
    if not reference_nos:
        return {}

    prices = {}
    try:
        refs_csv = ",".join(f'"{r}"' for r in reference_nos)
        response = httpx.get(
            DDF_URL,
            headers=READ_HEADERS,
            params={
                "select": "reference_no,price_aed",
                "source": "eq.Property Finder",
                "reference_no": f"in.({refs_csv})",
                "order": "scraped_at.desc",
            },
            timeout=15,
        )
        if response.status_code == 200:
            for row in response.json():
                ref = row.get("reference_no", "")
                if ref and ref not in prices:  # first = latest due to desc order
                    price = row.get("price_aed", 0)
                    if price:
                        prices[ref] = float(price)
            logger.info(f"DDF: fetched {len(prices)} existing prices")
        else:
            logger.warning(f"DDF price fetch failed: {response.status_code} — {response.text[:200]}")
    except Exception as e:
        logger.warning(f"DDF price fetch failed: {e}")

    return prices


def sync_to_ddf(listings: list[dict]) -> list[int]:
    """Map PF listings to ddf_listings schema and insert. Returns IDs of newly inserted rows."""
    if not listings:
        return []

    source = "Property Finder"
    city = "Dubai"

    # Get existing DDF prices for listing_change calculation
    ref_nos = [l["reference_no"] for l in listings if l.get("reference_no")]
    existing_prices = fetch_ddf_latest_prices(ref_nos)

    ddf_rows = []
    for l in listings:
        ref = l.get("reference_no", "")
        price_aed = int(l.get("price", 0) or 0)
        url = l.get("listing_url", "")
        scraped_at = l.get("scraped_at", "")

        dup_hash = compute_dup_hash(ref, source, price_aed, url)

        # listing_change: price difference from previous DDF row
        listing_change = None
        if ref in existing_prices and existing_prices[ref] > 0 and price_aed > 0:
            diff = price_aed - int(existing_prices[ref])
            if diff != 0:
                listing_change = diff

        # Map fields
        raw_type = l.get("listing_type", "")
        purpose = "Rent" if raw_type == "rent" else "Sale" if raw_type == "sale" else raw_type.capitalize()

        raw_prop_type = l.get("property_type", "")
        prop_type = raw_prop_type.capitalize() if raw_prop_type else ""

        date_listed = scraped_at[:10] if scraped_at else None

        ddf_rows.append({
            "reference_no": ref,
            "purpose": purpose,
            "type": prop_type,
            "community": l.get("community", ""),
            "property_name": l.get("building", ""),
            "bedrooms": "0" if l.get("bedrooms") == "Studio" else l.get("bedrooms", ""),
            "bathrooms": l.get("bathrooms") or None,
            "size_sqft": int(l.get("size_sqft", 0) or 0),
            "price_aed": price_aed,
            "url": url,
            "scraped_at": scraped_at,
            "source": source,
            "city": city,
            "date_listed": date_listed,
            "is_valid": True,
            "dup_hash": dup_hash,
            "listing_change": listing_change,
        })

    ddf_rows = sanitize_listings(ddf_rows)

    # Insert with ON CONFLICT (dup_hash) DO NOTHING — return inserted rows
    inserted_ids = []
    for i in range(0, len(ddf_rows), 50):
        batch = ddf_rows[i : i + 50]
        try:
            response = httpx.post(
                DDF_URL,
                headers=DDF_INSERT_HEADERS,
                json=batch,
                params={"on_conflict": "dup_hash"},
                timeout=30,
            )
            if response.status_code in (200, 201):
                rows = response.json() if response.text else []
                ids = [r["id"] for r in rows if "id" in r]
                inserted_ids.extend(ids)
                logger.info(f"DDF batch {i // 50 + 1}: inserted {len(ids)} new rows (sent {len(batch)})")
            else:
                logger.error(f"DDF batch {i // 50 + 1} failed: {response.status_code} — {response.text[:200]}")
        except Exception as e:
            logger.error(f"DDF batch {i // 50 + 1} failed: {e}")

    logger.info(f"DDF sync: {len(inserted_ids)} new rows inserted ({len(ddf_rows)} sent, rest skipped by dup_hash)")
    return inserted_ids


# ── Dip Computation ───────────────────────────────────────────────────────────


def compute_dip_for_row(row_id: int) -> bool:
    """Compute dip values for a single DDF listing by finding best matching prior listing."""
    try:
        # Fetch the row
        resp = httpx.get(
            DDF_URL,
            headers=READ_HEADERS,
            params={"select": "*", "id": f"eq.{row_id}"},
            timeout=15,
        )
        if resp.status_code != 200 or not resp.json():
            return False
        row = resp.json()[0]

        building = row.get("property_name")
        purpose = row.get("purpose")
        bedrooms = row.get("bedrooms")
        size = row.get("size_sqft")
        furnished = row.get("furnished")
        price = row.get("price_aed")
        date_listed = row.get("date_listed")

        if not building or not price or bedrooms is None or not date_listed:
            return False

        # Find matching prior listings
        params = {
            "select": "id,price_aed,date_listed,url,source,size_sqft,furnished,property_name",
            "property_name": f"eq.{building}",
            "purpose": f"eq.{purpose}",
            "bedrooms": f"eq.{bedrooms}",
            "date_listed": f"lt.{date_listed}",
            "price_aed": "gt.0",
            "order": "date_listed.desc",
            "limit": "50",
        }
        if furnished:
            params["furnished"] = f"eq.{furnished}"

        resp2 = httpx.get(DDF_URL, headers=READ_HEADERS, params=params, timeout=15)
        if resp2.status_code != 200:
            return False
        matches = resp2.json() or []

        # Filter by size ±10%
        if size and int(size) > 0:
            lo = int(size) * 0.9
            hi = int(size) * 1.1
            size_matches = [m for m in matches if m.get("size_sqft") and lo <= int(m["size_sqft"]) <= hi]
            if size_matches:
                matches = size_matches
            else:
                matches = [m for m in matches if not m.get("size_sqft")]

        if not matches:
            return False

        prev = matches[0]  # Most recent prior listing
        prev_price = prev.get("price_aed")
        if not prev_price:
            return False

        dip_pct = round(((price - prev_price) / prev_price) * 100, 1)
        dip_price = price - prev_price

        # Update the row with dip data
        update_resp = httpx.patch(
            DDF_URL,
            headers=DDF_UPDATE_HEADERS,
            params={"id": f"eq.{row_id}"},
            json={
                "dip_pct": dip_pct,
                "dip_price": dip_price,
                "dip_ref_id": prev["id"],
                "dip_prev_price": prev_price,
                "dip_prev_url": prev.get("url"),
                "dip_prev_source": prev.get("source"),
                "dip_prev_date": prev.get("date_listed"),
                "dip_prev_size": prev.get("size_sqft"),
                "dip_prev_furnished": prev.get("furnished"),
            },
            timeout=15,
        )
        if update_resp.status_code in (200, 204):
            logger.info(f"Dip computed for row {row_id}: {dip_pct}% (AED {dip_price:,})")
            return True
        else:
            logger.warning(f"Dip update failed for row {row_id}: {update_resp.status_code}")
            return False

    except Exception as e:
        logger.warning(f"Dip computation failed for row {row_id}: {e}")
        return False


def compute_dips_for_rows(row_ids: list[int]) -> int:
    """Compute dips for a list of newly inserted DDF row IDs."""
    if not row_ids:
        return 0
    logger.info(f"Computing dips for {len(row_ids)} new rows...")
    computed = 0
    for row_id in row_ids:
        if compute_dip_for_row(row_id):
            computed += 1
    logger.info(f"Dips computed: {computed}/{len(row_ids)}")
    return computed
