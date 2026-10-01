"""Detection and optional last-resort FlareSolverr integration."""
from __future__ import annotations

from typing import Any

from http_client import post_json

CHALLENGE_MARKERS = (
    "just a moment", "checking your browser", "cf-chl", "cf_chl_opt",
    "attention required! | cloudflare", "ddos protection by", "px-captcha",
    "perimeterx", "datadome", "access denied",
)


def is_challenge(status: int, headers: Any, body: str) -> bool:
    lowered_headers = {str(key).lower() for key in headers}
    if status in (403, 429, 503) and ({"cf-ray", "server"} & lowered_headers):
        return True
    low = (body or "")[:8000].lower()
    return any(marker in low for marker in CHALLENGE_MARKERS)


def flaresolverr(url: str, endpoint: str = "http://localhost:8191/v1", timeout: int = 70) -> str | None:
    try:
        response = post_json(endpoint, {"cmd": "request.get", "url": url, "maxTimeout": 60000}, timeout)
        if response.status_code == 200:
            payload = response.json()
            if payload.get("status") == "ok":
                return payload.get("solution", {}).get("response")
    except Exception:
        return None
    return None
