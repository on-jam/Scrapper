"""Jittered, per-host politeness gate."""
from __future__ import annotations

import random
import threading
import time
from urllib.parse import urlparse

_lock = threading.Lock()
_last: dict[str, float] = {}
MIN_INTERVAL = 1.0


def wait_for_slot(url: str, crawl_delay: float = 0.0) -> None:
    host = urlparse(url).netloc.lower()
    interval = max(MIN_INTERVAL, crawl_delay) * random.uniform(0.8, 1.5)
    with _lock:
        remaining = interval - (time.monotonic() - _last.get(host, 0.0))
        if remaining > 0:
            time.sleep(remaining)
        _last[host] = time.monotonic()
