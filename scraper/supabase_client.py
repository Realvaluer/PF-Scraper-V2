import os
import re
import json
import hashlib
import logging
import httpx

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"].strip()

# RealValuer Supabase (rv_sales, rv_rentals)
RV_SUPABASE_URL = os.environ.get("RV_SUPABASE_URL", "https://jbqxxaxesaqymqgtmkvu.supabase.co").strip()
RV_SUPABASE_KEY = os.environ.get("RV_SUPABASE_KEY", "").strip()

REST_URL = f"{SUPABASE_URL}/rest/v1/pf_listings_v2"
PRICE_HISTORY_URL = f"{SUPABASE_URL}/rest/v1/pf_price_history"
DDF_URL = f"{SUPABASE_URL}/rest/v1/ddf_listings"
RV_SALES_URL = f"{RV_SUPABASE_URL}/rest/v1/rv_sales"
RV_RENTALS_URL = f"{RV_SUPABASE_URL}/rest/v1/rv_rentals"

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

# RealValuer read headers
RV_READ_HEADERS = {
    "apikey": RV_SUPABASE_KEY,
    "Authorization": f"Bearer {RV_SUPABASE_KEY}",
    "Content-Type": "application/json",
} if RV_SUPABASE_KEY else {}


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
    PF_EXCLUDE = {"bathrooms", "scraped_at", "ready_off_plan", "furnished", "city", "category", "last_refreshed_at"}
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


def fetch_latest_listed_date(purpose: str, city: str) -> str:
    """Fetch the most recent listed_date for a given purpose+city from ddf_listings."""
    try:
        resp = httpx.get(
            DDF_URL,
            headers=READ_HEADERS,
            params={
                "select": "listed_date",
                "source": "eq.Property Finder",
                "purpose": f"eq.{purpose}",
                "city": f"eq.{city}",
                "is_valid": "eq.true",
                "listed_date": "not.is.null",
                "order": "listed_date.desc",
                "limit": "1",
            },
            timeout=15,
        )
        if resp.status_code == 200 and resp.json():
            return resp.json()[0].get("listed_date", "")
    except Exception as e:
        logger.warning(f"Failed to fetch latest listed_date: {e}")
    return ""


def fetch_ddf_latest_prices(reference_nos: list[str]) -> dict[str, tuple]:
    """Fetch latest price_aed and listed_date per reference_no from ddf_listings.
    Returns dict mapping reference_no -> (price, listed_date)."""
    if not reference_nos:
        return {}

    prices = {}
    try:
        refs_csv = ",".join(f'"{r}"' for r in reference_nos)
        response = httpx.get(
            DDF_URL,
            headers=READ_HEADERS,
            params={
                "select": "reference_no,price_aed,listed_date",
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
                    date = row.get("listed_date", "")
                    if price:
                        prices[ref] = (float(price), date)
            logger.info(f"DDF: fetched {len(prices)} existing prices")
        else:
            logger.warning(f"DDF price fetch failed: {response.status_code} — {response.text[:200]}")
    except Exception as e:
        logger.warning(f"DDF price fetch failed: {e}")

    return prices


def invalidate_old_ddf_rows(new_row_ids: list[int]) -> int:
    """For each newly inserted row, mark older rows with same reference_no + purpose as is_valid=false."""
    if not new_row_ids:
        return 0

    invalidated = 0

    # Fetch the new rows to get their reference_no + purpose
    for i in range(0, len(new_row_ids), 50):
        batch_ids = new_row_ids[i : i + 50]
        ids_csv = ",".join(str(rid) for rid in batch_ids)
        try:
            resp = httpx.get(
                DDF_URL,
                headers=READ_HEADERS,
                params={
                    "select": "id,reference_no,purpose",
                    "id": f"in.({ids_csv})",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning(f"Failed to fetch new rows for invalidation: {resp.status_code}")
                continue

            for row in resp.json():
                ref = row.get("reference_no")
                purpose = row.get("purpose")
                new_id = row.get("id")
                if not ref or not purpose:
                    continue

                # Mark all older rows for this ref+purpose as is_valid=false
                patch_resp = httpx.patch(
                    DDF_URL,
                    headers=DDF_UPDATE_HEADERS,
                    params={
                        "reference_no": f"eq.{ref}",
                        "purpose": f"eq.{purpose}",
                        "source": "eq.Property Finder",
                        "id": f"neq.{new_id}",
                        "is_valid": "eq.true",
                    },
                    json={"is_valid": False},
                    timeout=15,
                )
                if patch_resp.status_code in (200, 204):
                    invalidated += 1
                else:
                    logger.warning(f"Failed to invalidate old rows for {ref}: {patch_resp.status_code}")

        except Exception as e:
            logger.warning(f"Invalidation batch failed: {e}")

    logger.info(f"Invalidated old rows for {invalidated} listings")
    return invalidated


def sync_to_ddf(listings: list[dict]) -> list[int]:
    """Map PF listings to ddf_listings schema and insert. Returns IDs of newly inserted rows."""
    if not listings:
        return []

    source = "Property Finder"

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
        listing_change_date = None
        if ref in existing_prices:
            prev_price, prev_date = existing_prices[ref]
            if prev_price > 0 and price_aed > 0:
                diff = price_aed - int(prev_price)
                if diff != 0:
                    listing_change = diff
                    listing_change_date = prev_date

        # Map fields
        raw_type = l.get("listing_type", "")
        purpose = "Rent" if raw_type == "rent" else "Sale" if raw_type == "sale" else raw_type.capitalize()

        raw_prop_type = l.get("property_type", "")
        prop_type = raw_prop_type.capitalize() if raw_prop_type else ""

        # Use PF's listed_date (full timestamp) instead of scrape date
        listed_date_raw = l.get("listed_date", "")
        listed_date = listed_date_raw if listed_date_raw else (scraped_at[:10] if scraped_at else None)

        # Use PF's last_refreshed_at for listing_change_date if we detected a price change
        last_refreshed = l.get("last_refreshed_at", "")
        if listing_change and last_refreshed:
            listing_change_date = last_refreshed

        # furnished mapping
        furnished = l.get("furnished", "")

        # ready_off_plan mapping
        ready_off_plan = l.get("ready_off_plan", "")

        ddf_rows.append({
            "reference_no": ref,
            "purpose": purpose,
            "type": prop_type,
            "community": l.get("community", ""),
            "property_name": l.get("building", ""),
            "bedrooms": l.get("bedrooms", ""),
            "bathrooms": l.get("bathrooms") or None,
            "size_sqft": int(l.get("size_sqft", 0) or 0),
            "price_aed": price_aed,
            "url": url,
            "scraped_at": scraped_at,
            "source": source,
            "city": l.get("city", "Dubai"),
            "category": l.get("category", "Residential"),
            "listed_date": listed_date,
            "is_valid": True,
            "dup_hash": dup_hash,
            "listing_change": listing_change,
            "listing_change_date": listing_change_date,
            "ready_off_plan": ready_off_plan or None,
            "furnished": furnished or None,
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

    # Mark older rows for the same reference_no + purpose as is_valid = false
    if inserted_ids:
        invalidate_old_ddf_rows(inserted_ids)

    # Update existing rows with ready_off_plan and furnished (fields that may have been missing)
    _update_existing_ddf_fields(ddf_rows)

    return inserted_ids


def _update_existing_ddf_fields(ddf_rows: list[dict]) -> int:
    """Update existing DDF rows with fields that may have been missing."""
    updated = 0
    for row in ddf_rows:
        ref = row.get("reference_no")
        purpose = row.get("purpose")
        if not ref:
            continue

        patch = {}
        ready = row.get("ready_off_plan")
        furnished = row.get("furnished")
        listed_date = row.get("listed_date")
        bedrooms = row.get("bedrooms")

        if ready:
            patch["ready_off_plan"] = ready
        if furnished:
            patch["furnished"] = furnished
        if listed_date:
            patch["listed_date"] = listed_date
        if bedrooms == "Studio":
            patch["bedrooms"] = "Studio"

        if not patch:
            continue

        try:
            resp = httpx.patch(
                DDF_URL,
                headers=DDF_UPDATE_HEADERS,
                params={
                    "reference_no": f"eq.{ref}",
                    "purpose": f"eq.{purpose}",
                    "source": "eq.Property Finder",
                    "is_valid": "eq.true",
                },
                json=patch,
                timeout=10,
            )
            if resp.status_code in (200, 204):
                updated += 1
        except Exception:
            pass

    if updated:
        logger.info(f"Updated existing fields (listed_date/bedrooms/ready/furnished) on {updated} rows")
    return updated


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
        listed_date = row.get("listed_date")

        if not building or not price or bedrooms is None or not listed_date:
            return False

        # Find matching prior listings — try exact name, then split variant, then fuzzy
        matches = []
        search_names = [building]
        split_name = _split_name_numbers(building)
        if split_name != building:
            search_names.append(split_name)

        for search_name in search_names:
            params = {
                "select": "id,price_aed,listed_date,url,source,size_sqft,furnished,property_name",
                "property_name": f"eq.{search_name}",
                "purpose": f"eq.{purpose}",
                "bedrooms": f"eq.{bedrooms}",
                "listed_date": f"lt.{listed_date}",
                "price_aed": "gt.0",
                "order": "listed_date.desc",
                "limit": "50",
            }
            if furnished:
                params["furnished"] = f"eq.{furnished}"

            resp2 = httpx.get(DDF_URL, headers=READ_HEADERS, params=params, timeout=15)
            if resp2.status_code == 200 and resp2.json():
                matches = resp2.json()
                break

        # Fallback: fuzzy search (ilike) if exact matches failed
        if not matches:
            params = {
                "select": "id,price_aed,listed_date,url,source,size_sqft,furnished,property_name",
                "property_name": f"ilike.%{split_name}%",
                "purpose": f"eq.{purpose}",
                "bedrooms": f"eq.{bedrooms}",
                "listed_date": f"lt.{listed_date}",
                "price_aed": "gt.0",
                "order": "listed_date.desc",
                "limit": "50",
            }
            if furnished:
                params["furnished"] = f"eq.{furnished}"

            resp2 = httpx.get(DDF_URL, headers=READ_HEADERS, params=params, timeout=15)
            if resp2.status_code == 200:
                candidates = resp2.json() or []
                # Filter with fuzzy building name match
                matches = [m for m in candidates if _building_fuzzy_match(building, m.get("property_name", ""))]

        # Filter by size ±15%
        if size and int(size) > 0:
            lo = int(size) * 0.85
            hi = int(size) * 1.15
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
                "dip_prev_date": prev.get("listed_date"),
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


def backfill_dips() -> int:
    """Backfill dip computation for all Property Finder DDF rows with dip_pct IS NULL."""
    logger.info("=== Backfilling dips for all PF rows with NULL dip_pct ===")

    # Fetch all PF rows missing dip_pct
    all_ids = []
    offset = 0
    batch_size = 1000

    while True:
        try:
            resp = httpx.get(
                DDF_URL,
                headers=READ_HEADERS,
                params={
                    "select": "id",
                    "source": "eq.Property Finder",
                    "dip_pct": "is.null",
                    "order": "id.asc",
                    "limit": str(batch_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"Failed to fetch rows: {resp.status_code} — {resp.text[:200]}")
                break
            rows = resp.json()
            if not rows:
                break
            all_ids.extend(r["id"] for r in rows)
            offset += batch_size
            logger.info(f"Fetched {len(all_ids)} row IDs so far...")
        except Exception as e:
            logger.error(f"Failed to fetch rows: {e}")
            break

    logger.info(f"Total rows to backfill: {len(all_ids)}")

    computed = 0
    for i, row_id in enumerate(all_ids):
        if compute_dip_for_row(row_id):
            computed += 1
        if (i + 1) % 100 == 0:
            logger.info(f"Progress: {i + 1}/{len(all_ids)} processed, {computed} dips computed")

    logger.info(f"=== Backfill complete: {computed}/{len(all_ids)} dips computed ===")
    return computed


def cleanup_duplicates() -> int:
    """Find all reference_no + purpose combos with multiple is_valid=true rows, keep only the latest."""
    logger.info("=== Cleaning up duplicate is_valid rows ===")

    # Fetch all valid PF rows, ordered by scraped_at desc
    all_rows = []
    offset = 0
    batch_size = 1000

    while True:
        try:
            resp = httpx.get(
                DDF_URL,
                headers=READ_HEADERS,
                params={
                    "select": "id,reference_no,purpose,scraped_at",
                    "source": "eq.Property Finder",
                    "is_valid": "eq.true",
                    "order": "scraped_at.desc",
                    "limit": str(batch_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"Failed to fetch rows: {resp.status_code}")
                break
            rows = resp.json()
            if not rows:
                break
            all_rows.extend(rows)
            offset += batch_size
            logger.info(f"Fetched {len(all_rows)} rows so far...")
        except Exception as e:
            logger.error(f"Failed to fetch rows: {e}")
            break

    logger.info(f"Total valid PF rows: {len(all_rows)}")

    # Group by reference_no + purpose, find duplicates
    from collections import defaultdict
    groups = defaultdict(list)
    for row in all_rows:
        key = (row.get("reference_no", ""), row.get("purpose", ""))
        groups[key].append(row)

    ids_to_invalidate = []
    for key, rows in groups.items():
        if len(rows) > 1:
            # First row is latest (ordered by scraped_at desc), invalidate the rest
            for row in rows[1:]:
                ids_to_invalidate.append(row["id"])

    logger.info(f"Found {len(ids_to_invalidate)} duplicate rows to invalidate")

    # Invalidate in batches
    invalidated = 0
    for i in range(0, len(ids_to_invalidate), 50):
        batch = ids_to_invalidate[i : i + 50]
        ids_csv = ",".join(str(rid) for rid in batch)
        try:
            resp = httpx.patch(
                DDF_URL,
                headers=DDF_UPDATE_HEADERS,
                params={"id": f"in.({ids_csv})"},
                json={"is_valid": False},
                timeout=15,
            )
            if resp.status_code in (200, 204):
                invalidated += len(batch)
            else:
                logger.warning(f"Cleanup batch failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Cleanup batch failed: {e}")

    logger.info(f"=== Cleanup complete: {invalidated} duplicate rows invalidated ===")
    return invalidated


# ── Transaction Comparison (Listing vs DLD) ──────────────────────────────────


STOP_WORDS = {"dubai", "the", "al", "el", "de", "at", "in", "on", "by"}


def _normalize_building_name(s: str) -> str:
    """Normalize building name for comparison."""
    s = s.lower().strip()
    # Split concatenated name+number (DT1 → DT 1, BLVD2 → BLVD 2)
    s = re.sub(r'([A-Za-z])(\d)', r'\1 \2', s)
    s = re.sub(r'(\d)([A-Za-z])', r'\1 \2', s)
    s = s.replace('&', 'and')
    s = s.replace('-', ' ')
    s = re.sub(r'\btowers?\b', 'tower', s)
    s = re.sub(r'\bresidences?\b', 'residence', s)
    s = re.sub(r'\bbuildings?\b', '', s)
    s = re.sub(r'\bhouses?\b', 'house', s)
    s = re.sub(r"\bthe\b", '', s)
    # Strip sub-area suffixes for buildings like "Eden House Zaabeel" / "Eden House DIFC"
    s = re.sub(r"\b(zaabeel|za'abeel|difc|jlt|jvc|jvt|downtown|marina)\b", '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _extract_building_number(s: str) -> str:
    """Extract trailing number/letter from building name (e.g., '2' from 'Elitz 2', 'B' from 'Tower B')."""
    s = s.lower().strip()
    m = re.search(r'(\d+|[a-z])\s*$', s)
    return m.group(1) if m else ""


def _building_fuzzy_match(pf_name: str, rv_name: str) -> bool:
    """Fuzzy match two building names."""
    a = _normalize_building_name(pf_name)
    b = _normalize_building_name(rv_name)
    if a == b:
        return True
    if a in b or b in a:
        return True
    # Compare significant words
    wa = set(a.split())
    wb = set(b.split())
    if not wa or not wb:
        return False
    overlap = wa & wb
    smaller = min(len(wa), len(wb))
    if len(overlap) < max(2, smaller * 0.7):
        return False
    # If both names end with a number/letter, they must match
    num_a = _extract_building_number(a)
    num_b = _extract_building_number(b)
    if num_a and num_b and num_a != num_b:
        return False
    return True


def _community_fuzzy_match(community_a: str, community_b: str) -> bool:
    """Fuzzy match two community names. Returns True if they likely refer to the same place."""
    if not community_a or not community_b:
        return True  # If either is missing, don't filter on community

    # Normalize compound words
    def expand(s):
        s = s.lower().strip()
        s = re.sub(r'motorcity', 'motor city', s)
        s = re.sub(r'businessbay', 'business bay', s)
        return s

    a = expand(community_a)
    b = expand(community_b)

    # Exact match
    if a == b:
        return True

    # One contains the other
    if a in b or b in a:
        return True

    # Compare significant words (drop stop words)
    words_a = {w for w in a.replace("-", " ").split() if w not in STOP_WORDS and len(w) > 1}
    words_b = {w for w in b.replace("-", " ").split() if w not in STOP_WORDS and len(w) > 1}

    if not words_a or not words_b:
        return True

    # If majority of words overlap
    overlap = words_a & words_b
    smaller = min(len(words_a), len(words_b))
    return len(overlap) >= max(1, smaller * 0.5)


def _split_name_numbers(name: str) -> str:
    """Split concatenated name+number patterns: DT1 → DT 1, BLVD2 → BLVD 2, Tower3 → Tower 3."""
    return re.sub(r'([A-Za-z])(\d)', r'\1 \2', re.sub(r'(\d)([A-Za-z])', r'\1 \2', name))


def _search_rv_transactions(rv_url: str, building: str, bed_int: int) -> list[dict]:
    """Search RV for matching transactions using multi-strategy approach."""
    select = "id,price,date,size_sqft,community_name,property_name,bedrooms"

    # Only match actual sales (not mortgage) / new rentals (not renewals)
    is_sales = rv_url == RV_SALES_URL
    type_filter_key = "subtype" if is_sales else "rent_type_name"
    type_filter_val = "in.(Sale,Pre-Registration,Delayed Sale,Resale)" if is_sales else "eq.New"

    def _try_search(search_term: str, limit: int = 20) -> list[dict]:
        try:
            resp = httpx.get(rv_url, headers=RV_READ_HEADERS, params={
                "select": select,
                "property_name": f"ilike.%{search_term}%",
                "bedrooms": f"eq.{bed_int}",
                "is_valid": "eq.true",
                "price": "gt.0",
                type_filter_key: type_filter_val,
                "order": "date.desc",
                "limit": str(limit),
            }, timeout=15)
            if resp.status_code == 200 and resp.json():
                return resp.json()
        except Exception:
            pass
        return []

    # Strategy 1: full building name (case-insensitive)
    results = _try_search(building)
    if results:
        return results

    # Strategy 2: split concatenated names (DT1 → DT 1, BLVD2 → BLVD 2)
    split_name = _split_name_numbers(building)
    if split_name != building:
        results = _try_search(split_name)
        if results:
            return results

    # Strategy 3: progressively shorter name (3 words, 2 words, 1 word)
    words = split_name.split()
    for n_words in range(min(3, len(words)), 0, -1):
        search = " ".join(words[:n_words])
        if len(search) < 3:
            continue
        results = _try_search(search, limit=30)
        if results:
            return results

    return []


def compute_txn_for_row(row_id: int) -> bool:
    """Compute last transaction comparison for a single DDF listing."""
    if not RV_READ_HEADERS:
        logger.warning("RV_SUPABASE_KEY not set — skipping transaction comparison")
        return False

    try:
        # Fetch the DDF row
        resp = httpx.get(
            DDF_URL,
            headers=READ_HEADERS,
            params={"select": "id,property_name,community,purpose,bedrooms,size_sqft,price_aed", "id": f"eq.{row_id}"},
            timeout=15,
        )
        if resp.status_code != 200 or not resp.json():
            return False
        row = resp.json()[0]

        building = row.get("property_name")
        community = row.get("community", "")
        purpose = row.get("purpose")
        bedrooms = row.get("bedrooms")
        price = row.get("price_aed")
        size = row.get("size_sqft")

        if not building or not price or bedrooms is None:
            return False

        # Choose rv_sales or rv_rentals based on purpose
        if purpose == "Sale":
            rv_url = RV_SALES_URL
        elif purpose == "Rent":
            rv_url = RV_RENTALS_URL
        else:
            return False

        # Convert bedrooms: DDF stores "0" for Studio, RV stores 0 as integer
        try:
            bed_int = int(bedrooms)
        except (ValueError, TypeError):
            return False

        # Multi-strategy search for matching RV transactions
        matches = _search_rv_transactions(rv_url, building, bed_int)

        # Filter: fuzzy building name match
        matches = [m for m in matches if _building_fuzzy_match(building, m.get("property_name", ""))]

        # Filter: fuzzy community match
        matches = [m for m in matches if _community_fuzzy_match(community, m.get("community_name", ""))]

        # Filter by size ±15%
        if size and int(size) > 0:
            lo = int(size) * 0.85
            hi = int(size) * 1.15
            size_matches = [m for m in matches if m.get("size_sqft") and lo <= float(m["size_sqft"]) <= hi]
            if size_matches:
                matches = size_matches

        if not matches:
            return False

        txn = matches[0]  # Most recent matching transaction
        txn_price = float(txn["price"])
        if not txn_price:
            return False

        txn_change = price - txn_price
        txn_change_pct = round(((price - txn_price) / txn_price) * 100, 1)

        # Update DDF row with transaction comparison
        update_resp = httpx.patch(
            DDF_URL,
            headers=DDF_UPDATE_HEADERS,
            params={"id": f"eq.{row_id}"},
            json={
                "last_txn_price": txn_price,
                "last_txn_date": txn.get("date"),
                "last_txn_change": txn_change,
                "last_txn_change_pct": txn_change_pct,
            },
            timeout=15,
        )
        if update_resp.status_code in (200, 204):
            logger.info(f"Txn computed for row {row_id}: {txn_change_pct}% (AED {txn_change:,.0f}) vs {txn.get('date')}")
            return True
        else:
            logger.warning(f"Txn update failed for row {row_id}: {update_resp.status_code}")
            return False

    except Exception as e:
        logger.warning(f"Txn computation failed for row {row_id}: {e}")
        return False


def compute_txns_for_rows(row_ids: list[int]) -> int:
    """Compute transaction comparisons for a list of DDF row IDs."""
    if not row_ids:
        return 0
    if not RV_READ_HEADERS:
        logger.warning("RV_SUPABASE_KEY not set — skipping transaction comparisons")
        return 0
    logger.info(f"Computing transaction comparisons for {len(row_ids)} rows...")
    computed = 0
    for row_id in row_ids:
        if compute_txn_for_row(row_id):
            computed += 1
    logger.info(f"Transaction comparisons computed: {computed}/{len(row_ids)}")
    return computed


def backfill_txns() -> int:
    """Backfill transaction comparisons for all PF DDF rows with last_txn_price IS NULL."""
    if not RV_READ_HEADERS:
        logger.error("RV_SUPABASE_KEY not set — cannot backfill transactions")
        return 0

    logger.info("=== Backfilling transaction comparisons for all PF rows ===")

    all_ids = []
    offset = 0
    batch_size = 1000

    while True:
        try:
            resp = httpx.get(
                DDF_URL,
                headers=READ_HEADERS,
                params={
                    "select": "id",
                    "source": "eq.Property Finder",
                    "last_txn_price": "is.null",
                    "is_valid": "eq.true",
                    "order": "id.asc",
                    "limit": str(batch_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"Failed to fetch rows: {resp.status_code} — {resp.text[:200]}")
                break
            rows = resp.json()
            if not rows:
                break
            all_ids.extend(r["id"] for r in rows)
            offset += batch_size
            logger.info(f"Fetched {len(all_ids)} row IDs so far...")
        except Exception as e:
            logger.error(f"Failed to fetch rows: {e}")
            break

    logger.info(f"Total rows to backfill txns: {len(all_ids)}")

    computed = 0
    for i, row_id in enumerate(all_ids):
        if compute_txn_for_row(row_id):
            computed += 1
        if (i + 1) % 100 == 0:
            logger.info(f"Txn progress: {i + 1}/{len(all_ids)} processed, {computed} computed")

    logger.info(f"=== Txn backfill complete: {computed}/{len(all_ids)} computed ===")
    return computed


def reset_txns(limit: int = 0) -> list[int]:
    """Clear last_txn_* fields on PF DDF rows. If limit>0, only reset the most recent N rows."""
    if limit > 0:
        # Fetch the most recent N rows that have txn data
        logger.info(f"=== Resetting last_txn_* on most recent {limit} PF rows ===")
        try:
            resp = httpx.get(DDF_URL, headers=READ_HEADERS, params={
                "select": "id",
                "source": "eq.Property Finder",
                "is_valid": "eq.true",
                "last_txn_price": "not.is.null",
                "order": "id.desc",
                "limit": str(limit),
            }, timeout=30)
            if resp.status_code != 200 or not resp.json():
                logger.error(f"No rows found to reset")
                return []
            row_ids = [r["id"] for r in resp.json()]
        except Exception as e:
            logger.error(f"Failed to fetch rows: {e}")
            return []

        # Clear each row
        for rid in row_ids:
            httpx.patch(DDF_URL, headers=DDF_UPDATE_HEADERS, params={"id": f"eq.{rid}"}, json={
                "last_txn_price": None, "last_txn_date": None,
                "last_txn_change": None, "last_txn_change_pct": None,
            }, timeout=10)
        logger.info(f"Cleared last_txn_* on {len(row_ids)} rows")
        return row_ids
    else:
        logger.info("=== Resetting all last_txn_* fields on PF DDF rows ===")
        try:
            resp = httpx.patch(DDF_URL, headers=DDF_UPDATE_HEADERS, params={
                "source": "eq.Property Finder",
                "is_valid": "eq.true",
                "last_txn_price": "not.is.null",
            }, json={
                "last_txn_price": None, "last_txn_date": None,
                "last_txn_change": None, "last_txn_change_pct": None,
            }, timeout=30)
            if resp.status_code in (200, 204):
                logger.info("All last_txn_* fields cleared")
            else:
                logger.error(f"Reset failed: {resp.status_code} — {resp.text[:200]}")
        except Exception as e:
            logger.error(f"Reset failed: {e}")
        return []


# ── Delisted Detection ─────────────────────────────────────────────────────────


def detect_delisted(scraped_refs: set[str]) -> int:
    """Mark listings as delisted if they were not found in the latest deep refresh scrape.
    Only marks listings that were also not found in the PREVIOUS deep refresh (2-strike rule).
    Uses a 'missed_refreshes' counter on each row.

    Args:
        scraped_refs: Set of all reference_nos found during this deep refresh scrape.

    Returns:
        Number of listings marked as delisted.
    """
    if not scraped_refs:
        logger.warning("No scraped refs provided — skipping delisted detection")
        return 0

    logger.info(f"=== Delisted detection: checking against {len(scraped_refs):,} scraped refs ===")

    # Fetch all valid PF listings
    all_valid = []
    offset = 0
    batch_size = 1000

    while True:
        try:
            resp = httpx.get(
                DDF_URL,
                headers=READ_HEADERS,
                params={
                    "select": "id,reference_no,purpose",
                    "source": "eq.Property Finder",
                    "is_valid": "eq.true",
                    "order": "id.asc",
                    "limit": str(batch_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"Failed to fetch valid rows: {resp.status_code}")
                break
            rows = resp.json()
            if not rows:
                break
            all_valid.extend(rows)
            offset += batch_size
        except Exception as e:
            logger.error(f"Failed to fetch valid rows: {e}")
            break

    logger.info(f"Total valid PF listings in DDF: {len(all_valid):,}")

    # Find listings NOT in scraped refs
    missing_ids = []
    for row in all_valid:
        ref = row.get("reference_no", "")
        if ref and ref not in scraped_refs:
            missing_ids.append(row["id"])

    logger.info(f"Listings not found in this scrape: {len(missing_ids):,}")

    if not missing_ids:
        logger.info("All listings still active — no delistings")
        return 0

    # Mark as delisted (is_valid=false, set delisted_at timestamp)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    delisted = 0

    for i in range(0, len(missing_ids), 50):
        batch = missing_ids[i : i + 50]
        ids_csv = ",".join(str(rid) for rid in batch)
        try:
            resp = httpx.patch(
                DDF_URL,
                headers=DDF_UPDATE_HEADERS,
                params={"id": f"in.({ids_csv})"},
                json={"is_valid": False, "delisted_at": now},
                timeout=15,
            )
            if resp.status_code in (200, 204):
                delisted += len(batch)
            else:
                logger.warning(f"Delisted batch failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Delisted batch failed: {e}")

    logger.info(f"=== Delisted detection complete: {delisted:,} listings marked as delisted ===")
    return delisted
