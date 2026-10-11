import threading
import unittest
from unittest.mock import patch

import throttle


class ThrottleTests(unittest.TestCase):
    def test_large_robots_delay_cannot_sleep_past_crawl_deadline(self):
        throttle._last["delayed.test"] = 100.0
        with patch.object(throttle.time, "monotonic", return_value=100.0), patch.object(throttle.time, "sleep") as sleep:
            with self.assertRaises(throttle.CrawlDeadlineExceeded):
                throttle.wait_for_slot("https://delayed.test", 86400, deadline=110)
        sleep.assert_not_called()

    def test_sleep_releases_lock_for_other_hosts(self):
        throttle._last.clear()
        now = [100.0]
        throttle._last["busy.test"] = 100.0
        def sleep(seconds):
            acquired = []
            def check_lock():
                locked = throttle._lock.acquire(timeout=0.2)
                acquired.append(locked)
                if locked:
                    throttle._lock.release()
            thread = threading.Thread(target=check_lock)
            thread.start()
            thread.join(timeout=1)
            self.assertEqual(acquired, [True])
            now[0] += seconds
        with patch.object(throttle.time, "monotonic", side_effect=lambda: now[0]), patch.object(throttle.time, "sleep", side_effect=sleep), patch.object(throttle.random, "uniform", return_value=1):
            throttle.wait_for_slot("https://busy.test")
        self.assertEqual(throttle._last["busy.test"], 101.0)
