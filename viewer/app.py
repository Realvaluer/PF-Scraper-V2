import os
from datetime import datetime

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

app = FastAPI(title="PF Scraper V2 Viewer")


def format_number(n) -> str:
    if n is None:
        return "—"
    return f"{n:,.0f}"


@app.get("/", response_class=HTMLResponse)
def viewer(
    community: str = Query(default="all"),
    bedrooms: str = Query(default="all"),
    listing_type: str = Query(default="all"),
):
    # Fetch data
    query = supabase.table("pf_listings_v2").select("*").order("price", desc=False)
    if community != "all":
        query = query.eq("community", community)
    if bedrooms != "all":
        query = query.eq("bedrooms", bedrooms)
    if listing_type != "all":
        query = query.eq("listing_type", listing_type)

    result = query.limit(5000).execute()
    listings = result.data or []

    # Get distinct communities and last scraped time
    all_data = supabase.table("pf_listings_v2").select("community,scraped_at").execute()
    all_rows = all_data.data or []
    communities = sorted(set(r["community"] for r in all_rows if r.get("community")))
    last_scraped = ""
    if all_rows:
        times = [r["scraped_at"] for r in all_rows if r.get("scraped_at")]
        if times:
            last_scraped = max(times)

    # Get total count
    count_result = supabase.table("pf_listings_v2").select("id", count="exact").execute()
    total_count = count_result.count or 0

    # Build table rows
    rows_html = ""
    for l in listings:
        size = format_number(l.get("size_sqft"))
        price = format_number(l.get("price"))
        ppsf = format_number(l.get("price_per_sqft"))
        ltype = (l.get("listing_type") or "").capitalize()
        url = l.get("listing_url") or "#"
        rows_html += f"""
        <tr>
            <td>{l.get('community', '')}</td>
            <td>{l.get('building', '')}</td>
            <td>{l.get('bedrooms', '')}</td>
            <td>{size} sqft</td>
            <td>AED {price}</td>
            <td>AED {ppsf}/sqft</td>
            <td>{ltype}</td>
            <td><a href="{url}" target="_blank" rel="noopener">View</a></td>
        </tr>"""

    # Community options
    community_options = '<option value="all">All Communities</option>'
    for c in communities:
        selected = "selected" if c == community else ""
        community_options += f'<option value="{c}" {selected}>{c}</option>'

    # Bedrooms options
    bed_values = ["Studio", "1", "2", "3", "4", "5", "6", "7"]
    bed_options = '<option value="all">All Bedrooms</option>'
    for b in bed_values:
        selected = "selected" if b == bedrooms else ""
        bed_options += f'<option value="{b}" {selected}>{b}</option>'

    # Type options
    type_options = '<option value="all">All Types</option>'
    for t in ["sale", "rent"]:
        selected = "selected" if t == listing_type else ""
        type_options += f'<option value="{t}" {selected}>{t.capitalize()}</option>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PF Scraper V2 — Viewer</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #fff; color: #333; padding: 20px; }}
        h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
        .meta {{ color: #888; font-size: 0.85rem; margin-bottom: 16px; }}
        .filters {{ display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }}
        select {{ padding: 6px 12px; border: 1px solid #ccc; border-radius: 4px; font-size: 0.9rem; }}
        .count {{ font-size: 0.9rem; color: #555; margin-bottom: 12px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
        th {{ background: #f5f5f5; text-align: left; padding: 8px 10px; border-bottom: 2px solid #ddd; font-weight: 600; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #fafafa; }}
        a {{ color: #2563eb; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <h1>PF Scraper V2 — Listings Viewer</h1>
    <p class="meta">Total listings: {total_count} &nbsp;|&nbsp; Last scraped: {last_scraped or 'N/A'}</p>

    <form class="filters" method="get" action="/">
        <select name="community" onchange="this.form.submit()">{community_options}</select>
        <select name="bedrooms" onchange="this.form.submit()">{bed_options}</select>
        <select name="listing_type" onchange="this.form.submit()">{type_options}</select>
    </form>

    <p class="count">Showing {len(listings)} results</p>

    <table>
        <thead>
            <tr>
                <th>Community</th>
                <th>Building</th>
                <th>Bedrooms</th>
                <th>Size</th>
                <th>Price</th>
                <th>Price/sqft</th>
                <th>Type</th>
                <th>Listing</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
</body>
</html>"""

    return HTMLResponse(content=html)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
