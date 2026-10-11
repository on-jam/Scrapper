import unittest
from unittest.mock import Mock, patch

import rate_limits as limits


class CooldownTests(unittest.TestCase):
    def setUp(self):
        limits._until.clear()
        limits._deferred.clear()
        self.now = 1000.0
        self.sleeps = []
        self.addCleanup(patch.stopall)
        patch.object(limits.time, "monotonic", side_effect=lambda: self.now).start()
        patch.object(limits.time, "sleep", side_effect=self.sleep).start()
        patch.object(limits.random, "uniform", return_value=0).start()

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def response(self, status, retry=None):
        return Mock(status_code=status, headers={"Retry-After": retry} if retry else {})

    def test_retries_then_returns_success(self):
        limited, success = self.response(429), self.response(200)
        send = Mock(side_effect=[limited, success])
        self.assertIs(limits.request_with_cooldown(send, "https://school.test"), success)
        self.assertEqual(self.sleeps, [60])
        limited.close.assert_called_once()

    def test_exhaustion_does_not_keep_retrying_other_pages(self):
        send = Mock(side_effect=lambda: self.response(429))
        with self.assertRaises(limits.RateLimitDeferred):
            limits.request_with_cooldown(send, "https://school.test/page")
        self.assertEqual(send.call_count, 3)
        self.assertEqual(self.sleeps, [60, 120])
        with self.assertRaises(limits.RateLimitDeferred):
            limits.request_with_cooldown(send, "https://school.test/other")
        self.assertEqual(send.call_count, 3)
        other = self.response(200)
        self.assertIs(limits.request_with_cooldown(lambda: other, "https://other.test"), other)

    def test_long_retry_after_defers_without_early_retry(self):
        send = Mock(return_value=self.response(429, "3600"))
        with self.assertRaises(limits.RateLimitDeferred):
            limits.request_with_cooldown(send, "https://school.test")
        self.assertEqual(send.call_count, 1)
        self.assertEqual(self.sleeps, [])

    def test_http_date_and_invalid_header(self):
        from datetime import datetime, timedelta, timezone
        from email.utils import format_datetime
        header = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=600))
        self.assertGreater(limits.retry_delay(header), 590)
        self.assertEqual(limits.retry_delay("invalid"), 60)

    def test_other_statuses_are_not_retried(self):
        response = self.response(404)
        send = Mock(return_value=response)
        self.assertIs(limits.request_with_cooldown(send, "https://school.test"), response)
        send.assert_called_once()

    def test_nonfinite_retry_after_does_not_block_forever(self):
        self.assertEqual(limits.retry_delay("inf"), 60)
        self.assertEqual(limits.retry_delay("nan"), 60)

    def test_cooldown_respects_remaining_crawl_budget(self):
        send = Mock(return_value=self.response(429))
        with self.assertRaises(limits.RateLimitDeferred):
            limits.request_with_cooldown(send, "https://school.test", max_wait_seconds=10)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(self.sleeps, [])


if __name__ == "__main__":
    unittest.main()

