# PF-Scraper-V2

Playwright-based PropertyFinder scraper that bypasses AWS WAF JS challenges.

## Architecture

- **Scraper** (`scraper/scraper.py`) — Playwright + stealth, scrapes 5 communities (sale + rent), upserts to Supabase
- **Viewer** (`viewer/app.py`) — FastAPI web app showing scraped data with filters
- **Database** — Supabase `pf_listings_v2` table

## Communities Scraped

- Downtown Dubai
- Dubai Marina
- Business Bay
- Jumeirah Village Circle
- Palm Jumeirah

## Deployment (Railway)

- `pf-viewer-v2` — Always-on web service (port 8000)
- `pf-scraper-v2` — Cron service running at 02:00, 08:00, 14:00, 20:00 UTC

## Environment Variables

| Variable | Description |
|---|---|
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Supabase service role key |
