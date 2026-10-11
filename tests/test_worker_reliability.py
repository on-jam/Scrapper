import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests
import worker


class WorkerReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.worker = worker.DaigonWorker.__new__(worker.DaigonWorker)
        self.worker.journal = worker.Journal(self.root / "journal.sqlite3")
        self.addCleanup(self.worker.journal.connection.close)
        self.worker.upload_lock = threading.Lock()
        self.worker.worker_id = "test-worker"
        self.worker.api_url = "https://api.invalid"
        self.worker.session = Mock()
        self.worker.concurrency = 1
        self.worker.poll_seconds = 0.01
        self.worker.heartbeat_seconds = 0.01
        self.worker.once = False
        self.claim = {"work_item_id": "test-item", "lease_token": "token", "lease_version": 1}

    def saved_result(self, item=None):
        item = item or self.claim
        self.worker.journal.save_claim(item)
        result = {"success": True, "dossier": "Evidence", "lease_token": item["lease_token"], "lease_version": item["lease_version"]}
        path = self.root / (item["work_item_id"] + ".json")
        path.write_text(json.dumps(result), encoding="utf-8")
        self.worker.journal.save_result(item["work_item_id"], path)
        return path

    def test_stale_409_is_quarantined_without_deleting_evidence(self):
        path = self.saved_result()
        self.worker.session.post.return_value = Mock(status_code=409, text="This scraper lease no longer owns the work item")
        self.worker.flush_outbox()
        self.assertEqual(self.worker.journal.pending_results(), [])
        self.assertTrue(path.exists())
        self.worker.flush_outbox()
        self.assertEqual(self.worker.session.post.call_count, 1)

    def test_timeout_retains_outbox_and_stops_pass(self):
        self.saved_result()
        self.saved_result({**self.claim, "work_item_id": "other"})
        self.worker.session.post.side_effect = requests.Timeout("offline")
        self.worker.flush_outbox()
        self.assertEqual(len(self.worker.journal.pending_results()), 2)
        self.assertEqual(self.worker.session.post.call_count, 1)

    def test_crash_recovery_only_uses_matching_lease(self):
        path = self.saved_result()
        self.worker.journal.save_claim(self.claim)
        self.worker.journal.recover_results(self.root)
        self.assertEqual(len(self.worker.journal.pending_results()), 1)
        self.worker.journal.save_claim({**self.claim, "lease_token": "new", "lease_version": 2})
        self.worker.journal.recover_results(self.root)
        self.assertEqual(self.worker.journal.pending_results(), [])
        self.assertTrue(path.exists())

    def test_failed_task_does_not_kill_claim_loop(self):
        self.worker.claim = Mock(side_effect=[self.claim, self.claim, KeyboardInterrupt])
        self.worker.process = Mock(side_effect=[OSError("disk failure"), None])
        with self.assertRaises(KeyboardInterrupt):
            self.worker._run_claims()
        self.assertEqual(self.worker.process.call_count, 2)

    def test_heartbeat_continues_while_submitting(self):
        beat = threading.Event()
        self.worker.heartbeat = Mock(side_effect=lambda *args: beat.set())
        self.worker.scrape = Mock(return_value={"success": True, "dossier": "Evidence"})
        def submit(*args):
            self.assertTrue(beat.wait(1), "heartbeat stopped before result submission")
        self.worker.submit = Mock(side_effect=submit)
        with patch.object(worker, "RESULTS_DIR", self.root):
            self.worker.process(self.claim)
        self.assertEqual(self.worker.journal.pending_results(), [])

    def test_upload_outage_does_not_block_claims(self):
        uploading, claimed = threading.Event(), threading.Event()
        def flush():
            uploading.set()
            self.assertTrue(claimed.wait(1))
        def claims():
            self.assertTrue(uploading.wait(1))
            claimed.set()
        self.worker.flush_outbox = flush
        self.worker._run_claims = claims
        self.worker.run()

    def test_idle_slot_refills_while_another_school_is_still_running(self):
        self.worker.concurrency = 2
        self.worker.once = True
        second_started = threading.Event()
        self.worker.claim = Mock(side_effect=[self.claim, None, {**self.claim, "work_item_id": "second"}, None])
        def process(item):
            if item["work_item_id"] == "second":
                second_started.set()
            else:
                self.assertTrue(second_started.wait(1), "idle slot waited for the slow school")
        self.worker.process = process
        self.worker._run_claims()
        self.assertTrue(second_started.is_set())

    def test_empty_crawl_does_not_consume_ai_quota(self):
        self.worker.max_pages = None
        self.worker.timeout = 30
        self.worker.no_llm = False
        self.worker.model = "test-model"
        with patch.object(worker, "scrape_site", return_value=([], [])), patch.object(worker, "call_gemini") as ai:
            result = self.worker.scrape({**self.claim, "school": {"website": "https://school.invalid"}})
        self.assertFalse(result["success"])
        ai.assert_not_called()

    def test_crawl_error_fits_server_result_schema(self):
        self.worker.max_pages = None
        self.worker.timeout = 30
        self.worker.no_llm = True
        with patch.object(worker, "scrape_site", return_value=([], ["error " * 1000])):
            result = self.worker.scrape({**self.claim, "school": {"website": "https://school.invalid"}})
        self.assertLessEqual(len(result["error"]), 2000)
