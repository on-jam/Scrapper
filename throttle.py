"""Jittered, per-host politeness gate."""
from __future__ import annotations

import random
import math
import threading
import time
from urllib.parse import urlparse
from http_client import CrawlDeadlineExceeded

_lock = threading.Lock()
_last: dict[str, float] = {}
MIN_INTERVAL = 1.0


def wait_for_slot(url: str, crawl_delay: float = 0.0, *, deadline: float | None = None) -> None:
    host = urlparse(url).netloc.lower()
    if not math.isfinite(crawl_delay):
        raise ValueError("Non-finite crawl delay")
    interval = max(MIN_INTERVAL, crawl_delay) * random.uniform(0.8, 1.5)
    while True:
        with _lock:
            now = time.monotonic()
            last = _last.get(host)
            remaining = 0.0 if last is None else interval - (now - last)
            if deadline is not None and now + max(0.0, remaining) >= deadline:
                raise CrawlDeadlineExceeded("host crawl delay exceeds remaining crawl budget")
            if remaining <= 0:
                _last[host] = now
                return
        # Never hold the global lock while waiting for one host.
        time.sleep(remaining)
