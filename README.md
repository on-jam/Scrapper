# DAIGON remote dossier scraper

This is the office EliteDesk worker for the existing DAIGON dossier queue. AWS
keeps ownership of PostgreSQL, scheduling and dossier persistence. The worker
uses the existing scraper logic locally, while SQLite protects claimed work and
completed results across power or internet interruptions.

## Install

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
playwright install chromium
Copy-Item .env.example .env
```

Set `DAIGON_API_URL` and the Gemini key/model if AI dossier writing is
enabled. The scraper endpoints must be private behind Tailscale, a VPN, or an
IP allowlist because this deployment does not use a scraper token. Keep
`DAIGON_WORKER_DATA_DIR` on a local persistent disk if you relocate the project.

## Run

```powershell
python worker.py
python worker.py --once
```

The worker claims leases, heartbeats while scraping, writes each result to
`data/results/<work-item-id>.json`, and retries unsent results from
`data/worker.sqlite3`. An expired lease lets AWS safely reassign an unfinished
item. Results are submitted through the existing
`/integrations/external-scraper/{work_item_id}/result` endpoint, which uses the
normal dossier persistence pipeline.

Run one worker per PC. The worker uses concurrent jobs and adaptive crawl
stopping: it continues while high-value unexplored links remain, then stops
when dossier sections are covered or additional pages stop adding signal.
For testing, `--max-pages N` still provides an optional hard cap.

The crawler uses curl-cffi browser impersonation for school websites, persists
short-lived per-domain cookies under `data/sessions`, respects `robots.txt`
`Crawl-delay`, detects common anti-bot challenge pages, and optionally falls
back to a local FlareSolverr instance via `FLARESOLVERR_URL`. Playwright is
only used for sparse or challenged pages. JSON-LD, OpenGraph metadata,
hreflang, RSS/Atom feeds, and nested sitemaps are included in deterministic
extractions. API and Gemini calls still use `requests`.

## Throughput and rate limits

`CONCURRENCY = 6` in `worker.py` sets the number of schools processed at
once on each external machine; it is not configured through .env.
Page fetching within a school remains sequential;
navigation discovery and adaptive coverage stopping remain enabled.

Website and Gemini HTTP 429 responses retry twice, waiting at least 60 seconds
then 120 seconds plus jitter. Longer `Retry-After` seconds or HTTP dates take
precedence. Waits above 180 seconds and exhausted retries defer further requests
to that host until the cooldown expires. Other hosts remain independent. The
cooldown is process-local, so multiple machines still need sufficient shared
Gemini quota. This does not add a separate durable AI retry queue: exhausted
AI requests retain the deterministic dossier and its AI error.

For 50,000 successful dossiers in seven days, target at least 298 completions
per hour, preferably 360. At five minutes per successful school this requires
30 school slots, for example five machines at concurrency six (including any
internal scraper slots). Measure successful dossier throughput, not just claims;
failed sites and retries consume additional capacity.
