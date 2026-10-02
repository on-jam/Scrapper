"""Lease-based remote dossier worker for an office EliteDesk.

The AWS application owns the queue and database. This process only claims a
school, runs the existing scraper locally, journals the result in SQLite, and
submits it back when the API is reachable again.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import sqlite3
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from scraper import (
    build_deterministic_dossier,
    call_gemini,
    load_env_file,
    normalize_url,
    scrape_site,
)


LOG = logging.getLogger("daigon-scraper")
ROOT = Path(__file__).resolve().parent
load_env_file(ROOT / ".env")
DATA_DIR = Path(os.getenv("DAIGON_WORKER_DATA_DIR", ROOT / "data"))
RESULTS_DIR = DATA_DIR / "results"
JOURNAL_PATH = DATA_DIR / "worker.sqlite3"

LEASE_SECONDS = 3600
HEARTBEAT_SECONDS = 60
POLL_SECONDS = 5
CONCURRENCY = 4
MAX_PAGES = 80
REQUEST_TIMEOUT_SECONDS = 30
MAX_PDF_PAGES = 8
MAX_PAGES_PER_SECTION = 6
MAX_NEWS_PAGES = 8
LLM_MAX_CHARS = 70000


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class Journal:
    """Small durable outbox; no completed scrape is lost during a reboot."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS work_items (
                    work_item_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    result_path TEXT,
                    claimed_at TEXT,
                    lease_expires_at TEXT,
                    submitted_at TEXT,
                    last_error TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self.connection.commit()

    def save_claim(self, item: dict[str, Any]) -> None:
        with self.lock:
            self.connection.execute(
                """INSERT INTO work_items
                   (work_item_id, status, payload_json, claimed_at, lease_expires_at, updated_at)
                   VALUES (?, 'claimed', ?, ?, ?, ?)
                   ON CONFLICT(work_item_id) DO UPDATE SET
                   status='claimed', payload_json=excluded.payload_json,
                   claimed_at=excluded.claimed_at, lease_expires_at=excluded.lease_expires_at,
                   last_error=NULL, updated_at=excluded.updated_at""",
                (
                    item["work_item_id"],
                    json.dumps(item, default=str),
                    str(item.get("claimed_at") or utc_now()),
                    str(item.get("lease_expires_at") or ""),
                    utc_now(),
                ),
            )
            self.connection.commit()

    def save_result(self, work_item_id: str, result_path: Path) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE work_items SET status='pending_submit', result_path=?, updated_at=? WHERE work_item_id=?",
                (str(result_path), utc_now(), work_item_id),
            )
            self.connection.commit()

    def mark_submitted(self, work_item_id: str) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE work_items SET status='submitted', submitted_at=?, updated_at=? WHERE work_item_id=?",
                (utc_now(), utc_now(), work_item_id),
            )
            self.connection.commit()

    def mark_orphaned(self, work_item_id: str, error: str) -> None:
        """Stop retrying a result after the API says this worker lost ownership."""
        with self.lock:
            self.connection.execute(
                "UPDATE work_items SET status='orphaned', last_error=?, updated_at=? WHERE work_item_id=?",
                (error[:2000], utc_now(), work_item_id),
            )
            self.connection.commit()

    def mark_error(self, work_item_id: str, error: str) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE work_items SET last_error=?, updated_at=? WHERE work_item_id=?",
                (error[:2000], utc_now(), work_item_id),
            )
            self.connection.commit()

    def pending_results(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM work_items WHERE status='pending_submit' ORDER BY updated_at"
            ).fetchall()


class DaigonWorker:
    def __init__(self, once: bool = False) -> None:
        self.api_url = os.getenv("DAIGON_API_URL", "https://gtm.daigon.app").rstrip("/")
        self.worker_id = self._worker_id()
        self.lease_seconds = LEASE_SECONDS
        self.heartbeat_seconds = HEARTBEAT_SECONDS
        self.poll_seconds = POLL_SECONDS
        self.concurrency = CONCURRENCY
        self.max_pages = MAX_PAGES
        self.timeout = REQUEST_TIMEOUT_SECONDS
        self.no_llm = False
        self.model = os.getenv("DOSSIER_GEMINI_MODEL", os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite"))
        self.once = once
        self.session = requests.Session()
        scraper_token = os.getenv("DAIGON_SCRAPER_TOKEN", "").strip()
        if scraper_token:
            # Support the normal bearer convention and the explicit header
            # used by older DAIGON deployments.
            self.session.headers.update({
                "Authorization": f"Bearer {scraper_token}",
                "X-Scraper-Token": scraper_token,
            })
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        self.journal = Journal(JOURNAL_PATH)

    def _worker_id(self) -> str:
        path = DATA_DIR / "worker_id"
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        worker_id = f"{platform.node() or 'elitedesk'}-{uuid.uuid4().hex[:10]}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(worker_id, encoding="utf-8")
        return worker_id

    def _url(self, path: str) -> str:
        return f"{self.api_url}/integrations{path}"

    def claim(self) -> dict[str, Any] | None:
        response = self.session.post(
            self._url("/external-scraper/claim"),
            json={"worker_id": self.worker_id, "lease_seconds": self.lease_seconds},
            timeout=(10, 30),
        )
        response.raise_for_status()
        body = response.json()
        return body.get("work_item") if body.get("status") == "claimed" else None

    def heartbeat(self, work_item_id: str) -> None:
        response = self.session.post(
            self._url(f"/external-scraper/{work_item_id}/heartbeat"),
            json={"worker_id": self.worker_id, "lease_seconds": self.lease_seconds},
            timeout=(10, 30),
        )
        response.raise_for_status()

    def submit(self, work_item_id: str, result: dict[str, Any]) -> None:
        response = self.session.post(
            self._url(f"/external-scraper/{work_item_id}/result"),
            json={"worker_id": self.worker_id, **result},
            timeout=(10, 60),
        )
        if response.status_code == 409:
            detail = response.text.strip().replace("\n", " ")[:1000]
            raise RuntimeError(f"HTTP 409 Conflict for work item {work_item_id}: {detail or 'no response body'}")
        response.raise_for_status()

    def flush_outbox(self) -> None:
        for row in self.journal.pending_results():
            path = Path(row["result_path"])
            try:
                self.submit(row["work_item_id"], json.loads(path.read_text(encoding="utf-8")))
                self.journal.mark_submitted(row["work_item_id"])
                LOG.info("submitted queued result %s", row["work_item_id"])
            except Exception as error:  # noqa: BLE001
                message = str(error)
                if "does not own the work item" in message.lower():
                    self.journal.mark_orphaned(row["work_item_id"], message)
                    LOG.warning("discarding retry for orphaned work item %s: %s", row["work_item_id"], message)
                else:
                    self.journal.mark_error(row["work_item_id"], message)
                    LOG.warning("result upload deferred for %s: %s", row["work_item_id"], message)

    def scrape(self, item: dict[str, Any]) -> dict[str, Any]:
        school = item["school"]
        url = normalize_url(str(school.get("website") or ""))
        started = time.time()
        pages, errors = scrape_site(
            base_url=url,
            max_pages=self.max_pages,
            timeout=self.timeout,
            use_playwright=True,
            max_pdf_pages=MAX_PDF_PAGES,
            max_pages_per_section=MAX_PAGES_PER_SECTION,
            max_news_pages=MAX_NEWS_PAGES,
            browser_first=False,
        )
        deterministic = build_deterministic_dossier(school.get("name"), url, pages)
        llm_dossier: dict[str, Any] | None = None
        llm_error: str | None = None
        if not self.no_llm:
            try:
                llm_dossier = call_gemini(school.get("name"), url, pages, deterministic, self.model, LLM_MAX_CHARS)
            except Exception as error:  # noqa: BLE001
                llm_error = str(error)[:1000]
        raw = deterministic.get("sections", {})
        if llm_dossier:
            raw = {**raw, "_llm_structured_dossier": llm_dossier}
        result = {
            "school": {
                "id": school.get("id"),
                "name": school.get("name"),
                "website": school.get("website"),
                "normalized_domain": school.get("normalized_domain"),
                "country": school.get("country"),
            },
            "website_url": url,
            "success": bool(pages),
            "dossier": (llm_dossier or {}).get("dossier") or deterministic.get("research_brief"),
            "persona_context": "School research dossier produced by remote EliteDesk scraper",
            "urls_discovered": len({page.url for page in pages}),
            "source_urls": {page.section_hint: page.url for page in pages if page.section_hint},
            "scraped_sections": sorted({page.section_hint for page in pages if page.section_hint}),
            "useful_sections": sorted({page.section_hint for page in pages if page.word_count >= 40}),
            "raw_extractions": {"deterministic": raw, "crawl_errors": errors[:30], "llm_error": llm_error},
            "error": None if pages else ("No useful pages were scraped" + (f": {errors[0]}" if errors else "")),
            "worker": {"worker_id": self.worker_id, "elapsed_seconds": round(time.time() - started, 2)},
        }
        return result

    def process(self, item: dict[str, Any]) -> None:
        work_item_id = item["work_item_id"]
        self.journal.save_claim(item)
        stop = threading.Event()

        def keep_lease() -> None:
            while not stop.wait(self.heartbeat_seconds):
                try:
                    self.heartbeat(work_item_id)
                except Exception as error:  # noqa: BLE001
                    LOG.warning("heartbeat failed for %s: %s", work_item_id, error)

        thread = threading.Thread(target=keep_lease, name=f"heartbeat-{work_item_id}", daemon=True)
        thread.start()
        try:
            result = self.scrape(item)
        except Exception as error:  # noqa: BLE001
            result = {"success": False, "error": f"{type(error).__name__}: {error}"}
        finally:
            stop.set()
            thread.join(timeout=2)
        path = RESULTS_DIR / f"{work_item_id}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=True), encoding="utf-8")
        temporary.replace(path)
        self.journal.save_result(work_item_id, path)
        try:
            self.submit(work_item_id, result)
            self.journal.mark_submitted(work_item_id)
            LOG.info("completed %s", work_item_id)
        except Exception as error:  # noqa: BLE001
            self.journal.mark_error(work_item_id, str(error))
            LOG.warning("saved result for retry %s: %s", work_item_id, error)

    def run(self) -> None:
        LOG.info("worker %s online: api=%s concurrency=%s", self.worker_id, self.api_url, self.concurrency)
        with ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="scrape") as pool:
            active = set()
            while True:
                self.flush_outbox()
                # Refill each free slot immediately. Waiting for an entire
                # batch made fast sites sit idle behind one slow/blocked site.
                claim_failed = False
                while len(active) < self.concurrency:
                    try:
                        item = self.claim()
                    except Exception as error:  # noqa: BLE001
                        LOG.warning("claim failed: %s", error)
                        claim_failed = True
                        break
                    if not item:
                        break
                    active.add(pool.submit(self.process, item))

                if self.once and not active:
                    return
                if not active:
                    if claim_failed:
                        time.sleep(self.poll_seconds)
                    else:
                        time.sleep(self.poll_seconds)
                    continue

                done, active = wait(active, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
                if self.once and not active:
                    return
                if not active and claim_failed:
                    time.sleep(self.poll_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a lease-based DAIGON remote dossier worker.")
    parser.add_argument("--once", action="store_true", help="Claim and process currently available work, then exit.")
    parser.add_argument("--log-level", default=os.getenv("DAIGON_LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    DaigonWorker(once=args.once).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
