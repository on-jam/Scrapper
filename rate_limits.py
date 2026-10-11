"""Bounded 429 retries and process-local cooldowns for scraper endpoints."""
from __future__ import annotations

import logging
import math
import random
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import requests

_lock = threading.Lock()
_until: dict[str, float] = {}
_deferred: set[str] = set()


class RateLimitDeferred(requests.HTTPError):
    """The endpoint needs a longer pause; preserve data and defer this request."""


def retry_delay(value: str | None, attempt: int = 0) -> float:
    """Respect seconds and HTTP dates, with a one-minute exponential fallback."""
    delay = 60.0 * (2 ** attempt)
    if value:
        try:
            seconds = float(value)
            if math.isfinite(seconds):
                delay = max(delay, seconds)
        except ValueError:
            try:
                stamp = parsedate_to_datetime(value)
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                delay = max(delay, (stamp - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass
    return delay + random.uniform(0, 5)


def request_with_cooldown(send, url: str, *, retries: int = 2, max_wait_seconds: float = 300):
    """Retry only 429. Other hosts keep working; no secrets appear in errors.

    Short cooldowns wait locally. Longer cooldowns fail fast so callers can
    keep partial results. State is per process, not a fleet-wide quota limiter.
    """
    host = urlsplit(url).netloc.lower()
    wait_deadline = time.monotonic() + max_wait_seconds
    for attempt in range(retries + 1):
        while True:
            with _lock:
                remaining = _until.get(host, 0.0) - time.monotonic()
                deferred = host in _deferred
                if remaining <= 0:
                    _deferred.discard(host)
            if remaining > 180 or (remaining > 0 and deferred):
                raise RateLimitDeferred(f"HTTP 429: {host} cooldown; retry after {remaining:.0f}s")
            if remaining > 0 and time.monotonic() + remaining > wait_deadline:
                raise RateLimitDeferred(f"HTTP 429: {host} cooldown exceeds request wait budget")
            if remaining <= 0:
                break
            time.sleep(remaining)
        response = send()
        if response.status_code != 429:
            return response
        delay = retry_delay(response.headers.get("Retry-After"), attempt)
        response.close()
        logging.getLogger(__name__).warning(
            "HTTP 429 from %s: cooldown %.0fs (attempt %s/%s)",
            host, delay, attempt + 1, retries + 1,
        )
        with _lock:
            _until[host] = max(_until.get(host, 0.0), time.monotonic() + delay)
            if attempt == retries or delay > 180:
                _deferred.add(host)
        if attempt == retries or delay > 180:
            raise RateLimitDeferred(f"HTTP 429: {host} cooldown; retry after {delay:.0f}s")
    raise AssertionError("unreachable")

