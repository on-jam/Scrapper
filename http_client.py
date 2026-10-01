"""Browser-like HTTP transport for public-site crawling.

The DAIGON API and Gemini calls intentionally continue to use ``requests``;
this module is only for third-party school websites.
"""
from __future__ import annotations

from typing import Any

try:
    from curl_cffi import requests as cffi_requests
except ImportError:  # pragma: no cover - useful degraded mode before install
    cffi_requests = None

BROWSER_PROFILES = ("chrome124", "chrome123", "chrome120", "edge101", "safari17_0", "firefox133")
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
}


def make_session(profile: str = "chrome124") -> Any:
    if cffi_requests is None:
        import requests
        session = requests.Session()
    else:
        session = cffi_requests.Session(impersonate=profile)
    session.headers.update(DEFAULT_HEADERS)
    return session


def request(session: Any, method: str, url: str, timeout: int, **kwargs: Any) -> Any:
    kwargs.setdefault("timeout", (6, timeout))
    return session.request(method, url, **kwargs)


def post_json(url: str, payload: dict[str, Any], timeout: int = 70) -> Any:
    if cffi_requests is None:
        import requests
        return requests.post(url, json=payload, timeout=timeout)
    return cffi_requests.post(url, json=payload, timeout=timeout, headers=DEFAULT_HEADERS)
