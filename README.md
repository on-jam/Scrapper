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

Run one worker per PC. The worker currently uses five concurrent jobs and
scrapes up to 50 pages per school; change the constants at the top of
`worker.py` if a smaller EliteDesk needs a lighter profile.
