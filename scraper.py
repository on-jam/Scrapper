"""Standalone school dossier scraper test.

This script does not write to the DAIGON database or any external sales platform.
It crawls a school website, extracts useful text, optionally asks Gemini to write
a structured dossier, and saves the result under files/test_dossier_scrapes/.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "files" / "test_dossier_scrapes"

SCHOOL_PATHS = (
    "/",
    "/about",
    "/about-us",
    "/mission",
    "/vision",
    "/values",
    "/leadership",
    "/team",
    "/staff",
    "/curriculum",
    "/learning",
    "/academics",
    "/ib",
    "/cambridge",
    "/student-life",
    "/co-curricular",
    "/cocurricular",
    "/extracurricular",
    "/clubs",
    "/activities",
    "/sport",
    "/sports",
    "/technology",
    "/digital-learning",
    "/innovation",
    "/wellbeing",
    "/pastoral",
    "/admissions",
    "/university",
    "/careers",
    "/news",
    "/events",
    "/calendar",
    "/latest-news",
    "/blog",
    "/whats-on",
    "/our-school",
    "/why-us",
    "/school-life",
    "/learning-support",
    "/digital-citizenship",
    "/computer-science",
    "/steam",
    "/stem",
    "/enrichment",
    "/after-school",
    "/prospectus",
)

SECTION_KEYWORDS = {
    "mission_ethos": ("mission", "vision", "values", "ethos", "purpose", "belief"),
    "school_identity": ("international", "independent", "boarding", "day school", "private school", "ages"),
    "curriculum_skills": ("curriculum", "ib", "myp", "dp", "pyp", "cambridge", "gcse", "a level", "skills"),
    "co_curricular": ("co-curricular", "cocurricular", "extracurricular", "club", "clubs", "activities", "house", "sport"),
    "technology": ("technology", "digital", "stem", "steam", "computer science", "innovation", "robotics"),
    "wellbeing": ("wellbeing", "well-being", "pastoral", "belonging", "inclusion", "inclusive", "community"),
    "leadership": ("principal", "headteacher", "head of school", "director", "leadership", "governor"),
    "admissions_parent_positioning": ("admissions", "parents", "families", "prospectus", "fees", "open day"),
    "university_careers": ("university", "college", "careers", "pathways", "guidance", "alumni"),
    "recent_news": ("news", "event", "competition", "award", "calendar", "conference"),
}

NOISE_PATTERNS = (
    r"cookie policy",
    r"privacy policy",
    r"accept cookies",
    r"all rights reserved",
    r"skip to content",
    r"menu",
    r"casino",
    r"gambling",
    r"poker",
    r"slot game",
)

DATE_PATTERN = re.compile(
    r"\b(?:\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{4}|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}|"
    r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4})\b",
    flags=re.I,
)

NEWS_URL_PATTERN = re.compile(r"(news|blog|event|story|stories|latest|article|post|whats-on)", flags=re.I)


@dataclass
class ScrapedPage:
    url: str
    title: str
    section_hint: str
    date_hint: str | None
    word_count: int
    text: str
    content_type: str = "text/html"


class TextAndLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self._skip_depth = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1
            return
        if tag == "title":
            self._in_title = True
        if tag == "a":
            for name, value in attrs:
                if name.lower() == "href" and value:
                    self.links.append(value)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript", "svg"} and self._skip_depth:
            self._skip_depth -= 1
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        value = clean_text(data)
        if not value:
            return
        if self._in_title:
            self.title_parts.append(value)
        self.text_parts.append(value)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def clean_text(value: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip()
    if not value:
        return ""
    lowered = value.lower()
    if any(re.search(pattern, lowered) for pattern in NOISE_PATTERNS):
        return ""
    return value


def normalize_url(url: str, base_url: str | None = None) -> str:
    value = str(url or "").strip()
    if value and not base_url and not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    absolute = urljoin(base_url or value, value)
    parsed = urlparse(absolute)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return urlunparse((scheme, netloc, path, "", "", ""))


def same_site(url: str, base_url: str) -> bool:
    return urlparse(url).netloc.lower().removeprefix("www.") == urlparse(base_url).netloc.lower().removeprefix("www.")


def section_hint_for(url: str, text: str = "") -> str:
    haystack = f"{url} {text[:1000]}".lower()
    scores = {
        section: sum(1 for keyword in keywords if keyword in haystack)
        for section, keywords in SECTION_KEYWORDS.items()
    }
    best_section, best_score = max(scores.items(), key=lambda item: item[1])
    return best_section if best_score else "general"


def date_hint_for(url: str, title: str, text: str) -> str | None:
    for value in (url, title, text[:1200]):
        match = DATE_PATTERN.search(value or "")
        if match:
            return match.group(0)
    return None


def score_url(url: str) -> int:
    lowered = url.lower()
    score = 0
    for keywords in SECTION_KEYWORDS.values():
        score += sum(4 for keyword in keywords if keyword.replace(" ", "-") in lowered or keyword in lowered)
    if lowered.endswith(".pdf"):
        score += 8
    if any(token in lowered for token in ("about", "mission", "vision", "curriculum", "co-curricular", "activities", "wellbeing", "pastoral", "leadership")):
        score += 10
    if any(token in lowered for token in ("news", "blog", "event", "calendar")):
        score += 5
    if any(token in lowered for token in ("login", "policy", "privacy", "terms", "cookie", "wp-json", "tag/", "author/")):
        score -= 8
    return score


def is_crawlable_url(url: str, base_url: str) -> bool:
    if not same_site(url, base_url):
        return False
    lowered = url.lower()
    if any(token in lowered for token in ("mailto:", "tel:", "javascript:", "#", "/wp-json", "/feed", "replytocom", "/tag/", "/author/", "/login")):
        return False
    if any(lowered.endswith(suffix) for suffix in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".css", ".js", ".ico", ".zip")):
        return False
    return True


def score_news_url(url: str) -> int:
    lowered = url.lower()
    score = 0
    if NEWS_URL_PATTERN.search(lowered):
        score += 10
    if DATE_PATTERN.search(lowered):
        score += 12
    year_matches = re.findall(r"\b20\d{2}\b", lowered)
    if year_matches:
        score += max(int(year) - 2020 for year in year_matches)
    if any(token in lowered for token in ("category", "tag", "author", "page/", "feed", "replytocom")):
        score -= 8
    return score


def build_candidate_urls(base_url: str, sitemap_urls: list[str]) -> list[str]:
    guessed = [normalize_url(path, base_url) for path in SCHOOL_PATHS]
    all_urls = [base_url, *guessed, *sitemap_urls]
    deduped = sorted(set(all_urls), key=lambda url: score_url(url), reverse=True)
    return [url for url in deduped if is_crawlable_url(url, base_url) or url.lower().endswith(".pdf")]


def looks_like_news_index(url: str, section_hint: str) -> bool:
    lowered = url.lower()
    return section_hint == "recent_news" or any(token in lowered for token in ("/news", "/blog", "/events", "/latest", "/whats-on"))


def should_keep_candidate(url: str, section_counts: dict[str, int], max_pages_per_section: int, max_news_pages: int) -> bool:
    section = section_hint_for(url)
    if section == "recent_news":
        return section_counts.get(section, 0) < max_news_pages
    if section != "general":
        return section_counts.get(section, 0) < max_pages_per_section
    return True


def fetch(session: requests.Session, url: str, timeout: int) -> requests.Response:
    return session.get(
        url,
        timeout=(6, timeout),
        headers={
            "User-Agent": "Mozilla/5.0 DAIGON-Dossier-Test/1.0",
            "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
        },
    )


def parse_html(html: str, page_url: str) -> tuple[str, str, list[str]]:
    parser = TextAndLinkParser()
    parser.feed(html)
    title = clean_text(" ".join(parser.title_parts))
    text = clean_text(" ".join(parser.text_parts))
    links = [normalize_url(link, page_url) for link in parser.links if not link.startswith(("mailto:", "tel:", "#"))]
    return title, text, links


def extract_pdf_text(content: bytes, max_pages: int) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        return ""
    import io

    reader = PdfReader(io.BytesIO(content))
    parts: list[str] = []
    for page in reader.pages[:max_pages]:
        parts.append(page.extract_text() or "")
    return clean_text(" ".join(parts))


async def render_with_playwright(url: str, timeout: int) -> str | None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return None

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(user_agent="Mozilla/5.0 DAIGON-Dossier-Test/1.0")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
            await page.wait_for_timeout(1200)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(500)
            return await page.content()
        finally:
            await browser.close()


def render_page(url: str, timeout: int) -> str | None:
    try:
        return asyncio.run(render_with_playwright(url, timeout))
    except Exception:
        return None


def discover_sitemap_urls(session: requests.Session, base_url: str, timeout: int) -> list[str]:
    sitemap_roots = [urljoin(base_url, "/sitemap.xml"), urljoin(base_url, "/sitemap_index.xml")]
    found: list[str] = []
    visited: set[str] = set()
    queue = sitemap_roots[:]
    while queue and len(visited) < 8 and len(found) < 100:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            response = fetch(session, url, timeout)
            if response.status_code >= 400:
                continue
            locs = re.findall(r"<loc>\s*([^<]+)\s*</loc>", response.text, flags=re.I)
        except requests.RequestException:
            continue
        for loc in locs:
            loc = normalize_url(loc)
            if loc.endswith(".xml") and same_site(loc, base_url):
                queue.append(loc)
            elif same_site(loc, base_url):
                found.append(loc)
    return found


def add_news_drilldown_links(
    candidates: list[str],
    links: list[str],
    base_url: str,
    seen: set[str],
    section_counts: dict[str, int],
    max_news_pages: int,
) -> None:
    if section_counts.get("recent_news", 0) >= max_news_pages:
        return
    news_links = [
        link
        for link in set(links)
        if link not in seen and same_site(link, base_url) and score_news_url(link) > 0
    ]
    news_links = sorted(news_links, key=score_news_url, reverse=True)
    for link in news_links[: max(0, max_news_pages - section_counts.get("recent_news", 0)) * 3]:
        if link not in candidates:
            candidates.insert(0, link)


def scrape_site(
    base_url: str,
    max_pages: int,
    timeout: int,
    use_playwright: bool,
    max_pdf_pages: int,
    max_pages_per_section: int,
    max_news_pages: int,
    browser_first: bool,
) -> tuple[list[ScrapedPage], list[str]]:
    session = requests.Session()
    normalized_base = normalize_url(base_url)
    errors: list[str] = []
    sitemap_urls = discover_sitemap_urls(session, normalized_base, timeout)
    candidates = build_candidate_urls(normalized_base, sitemap_urls)
    seen: set[str] = set()
    pages: list[ScrapedPage] = []
    section_counts: dict[str, int] = {}

    while candidates and len(pages) < max_pages:
        url = candidates.pop(0)
        if url in seen:
            continue
        if not should_keep_candidate(url, section_counts, max_pages_per_section, max_news_pages):
            continue
        seen.add(url)
        try:
            response = fetch(session, url, timeout)
        except requests.RequestException as error:
            errors.append(f"{url}: {error}")
            continue
        if response.status_code >= 400:
            errors.append(f"{url}: HTTP {response.status_code}")
            continue

        content_type = response.headers.get("content-type", "").split(";")[0].lower()
        text = ""
        title = ""
        links: list[str] = []
        if url.lower().endswith(".pdf") or content_type == "application/pdf":
            text = extract_pdf_text(response.content, max_pdf_pages)
            title = Path(urlparse(url).path).name
            content_type = "application/pdf"
        else:
            html = None
            if use_playwright and browser_first:
                rendered = render_page(url, timeout)
                if rendered:
                    html = rendered
            if html is None:
                html = response.text
            title, text, links = parse_html(html, url)
            if use_playwright and len(text.split()) < 120:
                rendered = render_page(url, timeout)
                if rendered:
                    title, text, links = parse_html(rendered, url)
                else:
                    errors.append(f"{url}: playwright unavailable or failed; used HTTP response")

        word_count = len(text.split())
        if word_count < 40:
            continue
        section_hint = section_hint_for(url, text)
        section_counts[section_hint] = section_counts.get(section_hint, 0) + 1
        pages.append(
            ScrapedPage(
                url=url,
                title=title,
                section_hint=section_hint,
                date_hint=date_hint_for(url, title, text),
                word_count=word_count,
                text=text[:8000],
                content_type=content_type or "text/html",
            )
        )
        for link in sorted(set(links), key=lambda item: score_url(item), reverse=True):
            if (
                link not in seen
                and is_crawlable_url(link, normalized_base)
                and score_url(link) > 0
                and should_keep_candidate(link, section_counts, max_pages_per_section, max_news_pages)
            ):
                candidates.append(link)
            elif link not in seen and is_crawlable_url(link, normalized_base) and link not in candidates:
                candidates.append(link)
        if looks_like_news_index(url, section_hint):
            add_news_drilldown_links(candidates, links, normalized_base, seen, section_counts, max_news_pages)

    return pages, errors


def sentence_matches(section: str, sentence: str) -> bool:
    lowered = sentence.lower()
    return any(keyword in lowered for keyword in SECTION_KEYWORDS[section])


def build_deterministic_dossier(name: str | None, base_url: str, pages: list[ScrapedPage]) -> dict[str, Any]:
    sections: dict[str, dict[str, Any]] = {}
    evidence: list[dict[str, Any]] = []
    evidence_index = 1
    for section in SECTION_KEYWORDS:
        hits: list[dict[str, str]] = []
        for page in pages:
            sentences = re.split(r"(?<=[.!?])\s+", page.text)
            for sentence in sentences:
                sentence = clean_text(sentence)
                if 50 <= len(sentence) <= 300 and sentence_matches(section, sentence):
                    hits.append({"quote": sentence, "url": page.url})
                    evidence.append(
                        {
                            "id": f"scrape_{evidence_index:03d}",
                            "section": section,
                            "quote": sentence,
                            "url": page.url,
                            "shareable": True,
                        }
                    )
                    evidence_index += 1
                    break
            if len(hits) >= 3:
                break
        sections[section] = {
            "summary": " ".join(hit["quote"] for hit in hits[:2])[:700],
            "evidence": hits,
        }

    covered_sections = sum(1 for section in sections.values() if section["evidence"])
    score = min(100, 20 + covered_sections * 8 + min(len(pages), 10) * 2)
    grade = "high" if score >= 75 else "medium" if score >= 50 else "low"
    domain = urlparse(base_url).netloc.lower().removeprefix("www.")
    return {
        "school_name": name,
        "domain": domain,
        "generated_at": datetime.now(UTC).isoformat(),
        "quality": {
            "score": score,
            "grade": grade,
            "covered_sections": covered_sections,
            "page_count": len(pages),
        },
        "sections": sections,
        "evidence": evidence[:30],
        "research_brief": build_research_brief(name, pages, sections),
    }


def build_research_brief(name: str | None, pages: list[ScrapedPage], sections: dict[str, Any]) -> str:
    school = name or "this school"
    strongest = [
        (section, data["summary"])
        for section, data in sections.items()
        if data.get("summary")
    ][:8]
    lines = [f"Research brief for {school} based on {len(pages)} website pages."]
    for section, summary in strongest:
        label = section.replace("_", " ")
        lines.append(f"- {label}: {summary[:350]}")
    return "\n".join(lines)


def gemini_key() -> str | None:
    return os.getenv("SCRAPER_AI_KEY")


def parse_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            raise
        return json.loads(match.group(0))


def call_gemini(name: str | None, base_url: str, pages: list[ScrapedPage], deterministic: dict[str, Any], model: str, max_chars: int) -> dict[str, Any]:
    key = gemini_key()
    if not key:
        return {"status": "skipped", "reason": "No SCRAPER_AI_KEY found."}

    page_bundle = "\n\n".join(
        (
            f"SOURCE URL: {page.url}\n"
            f"PAGE TITLE: {page.title}\n"
            f"SECTION HINT: {page.section_hint}\n"
            f"DATE HINT: {page.date_hint or 'none'}\n"
            f"CONTENT TYPE: {page.content_type}\n"
            f"TEXT:\n{page.text}"
        )
        for page in pages
    )[:max_chars]
    prompt = f"""
You are a research analyst building a school research dossier for DAIGON Esports, an after-school esports club provider for international schools that positions itself around skills development: 21st century skills, problem-solving, teamwork, communication, and resilience rather than competitive gaming.

You will receive structured data extracted from multiple pages of one school's website. Your job is to consolidate this into a single comprehensive dossier that an AI copywriter will use to write personalized cold emails to staff at this school.

The dossier must be FACTUAL and grounded in the source data. Do not invent. If a section has no useful data, write "Not found in available pages."

Return valid JSON only. The `dossier` field must be plain text with the exact headers below. Do not put the instructions or descriptions under the headers. Under each header, write only factual findings or "Not found in available pages." Do not write lines like "Full name, location, founding..." or "This is critical for DAIGON."

## SCHOOL IDENTITY

## ETHOS & POSITIONING

## LEADERSHIP TEAM

## CO-CURRICULAR LANDSCAPE

## TECH STACK & DIGITAL POSTURE

## WELLBEING & PASTORAL

## SKILLS FRAMEWORKS REFERENCED

## RECENT NEWS & SIGNALS (last 12-24 months)

DATE REQUIREMENT: Every news item MUST include a date. If the source data has a date, include it. If the source data has the post but no extracted date, write the title followed by "(date not extracted - visit the post for date)". DO NOT silently omit dates. If no recent news is available at all, write "No recent news found in available pages."

## STRATEGIC DIRECTION

## RESEARCH BRIEFS

## PERSONALIZATION NOTES

## SOURCE URLS USED

## FLAGS / DISQUALIFIERS

Return this JSON shape:
{{
  "dossier": "plain text dossier with the exact headers above",
  "school_identity": {{"full_name": "", "location": "", "founding": "", "size": "", "campuses": "", "curriculum": "", "accreditations": "", "associations": "", "language_of_instruction": ""}},
  "ethos_positioning": {{"summary": "", "voice": "", "short_quotes": []}},
  "leadership_team": [{{"name": "", "title": "", "source_url": ""}}],
  "co_curricular_landscape": {{"program_structure": "", "named_activities": [], "external_providers": [], "tech_creative_clubs": [], "coordinator": "", "fees": ""}},
  "tech_stack_digital_posture": {{"device_program": "", "lms": "", "computer_science": "", "innovation_spaces": "", "digital_citizenship": "", "esports_gaming_references": ""}},
  "wellbeing_pastoral": {{"summary": "", "frameworks": [], "named_leads": [], "screen_time_or_gaming_views": ""}},
  "skills_frameworks_referenced": [],
  "recent_news_signals": [{{"date": "", "title": "", "signal": "", "source_url": ""}}],
  "strategic_direction": [],
  "research_briefs": [{{"brief": "", "source_url": "", "why_it_matters_for_personalization": ""}}],
  "personalization_notes": [{{"persona": "", "note": "", "source_url": ""}}],
  "source_urls_used": [],
  "flags_disqualifiers": [],
  "quality": {{"grade": "low|medium|high", "score": 0, "missing_important_sections": []}}
}}

Be concise and factual. The copywriter does not need fluff; they need usable signal.

School name hint: {name or ""}
Website: {base_url}

Deterministic pre-read from the crawler:
{json.dumps(deterministic, ensure_ascii=True)[:12000]}

Website pages:
{page_bundle}
""".strip()

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    response = requests.post(
        endpoint,
        params={"key": key},
        json={
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "response_mime_type": "application/json"},
        },
        timeout=(10, 90),
    )
    response.raise_for_status()
    payload = response.json()
    return parse_json_object(payload["candidates"][0]["content"]["parts"][0]["text"])


def load_school_from_db(school_id: str) -> dict[str, str | None]:
    sys.path.insert(0, str(ROOT))
    from daigon.db import Database  # noqa: PLC0415
    from daigon.models import School  # noqa: PLC0415

    with Database().session() as session:
        school = session.get(School, school_id)
        if not school:
            raise ValueError(f"School not found: {school_id}")
        return {
            "id": str(school.id),
            "name": school.name,
            "website": getattr(school, "website", None),
            "domain": getattr(school, "domain", None),
            "country": getattr(school, "country", None),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test the DAIGON school dossier scraper without DB writes.")
    parser.add_argument("--url", help="School website URL.")
    parser.add_argument("--school-id", help="Optional DAIGON school UUID to load website/name from DB.")
    parser.add_argument("--name", help="Optional school name hint.")
    parser.add_argument("--country", help="Optional country hint.")
    parser.add_argument("--max-pages", type=int, default=12)
    parser.add_argument("--max-pages-per-section", type=int, default=3)
    parser.add_argument("--max-news-pages", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--max-pdf-pages", type=int, default=6)
    parser.add_argument("--requests-only", action="store_true", help="Disable Playwright rendering fallback.")
    parser.add_argument("--http-first", action="store_true", help="Use HTTP first, then Playwright only for sparse pages.")
    parser.add_argument("--no-llm", action="store_true", help="Skip Gemini and output deterministic extraction only.")
    parser.add_argument("--model", default=os.getenv("DOSSIER_GEMINI_MODEL") or os.getenv("GEMINI_MODEL") or "gemini-3.6-flash")
    parser.add_argument("--llm-max-chars", type=int, default=60000)
    parser.add_argument("--output", help="Output JSON path. Defaults to files/test_dossier_scrapes/<domain>_<timestamp>.json.")
    return parser.parse_args()


def main() -> int:
    load_env_file(ROOT / ".env")
    args = parse_args()

    db_school: dict[str, str | None] = {}
    if args.school_id:
        db_school = load_school_from_db(args.school_id)

    url = args.url or db_school.get("website")
    if not url and db_school.get("domain"):
        url = f"https://{db_school['domain']}"
    if not url:
        raise SystemExit("Provide --url or a --school-id with website/domain.")
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    name = args.name or db_school.get("name")
    started = time.time()
    pages, errors = scrape_site(
        base_url=url,
        max_pages=max(1, args.max_pages),
        timeout=args.timeout,
        use_playwright=not args.requests_only,
        max_pdf_pages=args.max_pdf_pages,
        max_pages_per_section=max(1, args.max_pages_per_section),
        max_news_pages=max(0, args.max_news_pages),
        browser_first=not args.http_first,
    )
    deterministic = build_deterministic_dossier(name, normalize_url(url), pages)

    llm_dossier: dict[str, Any] | None = None
    llm_error: str | None = None
    if not args.no_llm:
        try:
            llm_dossier = call_gemini(
                name=name,
                base_url=normalize_url(url),
                pages=pages,
                deterministic=deterministic,
                model=args.model,
                max_chars=args.llm_max_chars,
            )
        except Exception as error:  # noqa: BLE001 - test script should preserve scrape output even if AI fails.
            llm_error = str(error)

    domain = urlparse(normalize_url(url)).netloc.lower().removeprefix("www.") or "school"
    raw_extractions = deterministic.get("sections", {})
    if llm_dossier:
        raw_extractions = {**raw_extractions, "_llm_structured_dossier": llm_dossier}
    school_cache_record = {
        "domain": domain,
        "school_name": name,
        "scraped_at": datetime.now(UTC).isoformat(),
        "persona_context": "School research dossier test",
        "urls_discovered": len({page.url for page in pages}),
        "categories": {
            page.section_hint: page.url
            for page in pages
            if page.section_hint
        },
        "categories_scraped": sorted({page.section_hint for page in pages if page.section_hint}),
        "categories_with_useful_data": sorted({page.section_hint for page in pages if page.word_count >= 40}),
        "blog_posts_scraped": sum(
            1 for page in pages
            if page.section_hint == "recent_news" or looks_like_news_index(page.url, page.section_hint)
        ),
        "raw_extractions": raw_extractions,
        "dossier": (llm_dossier or {}).get("dossier") or deterministic.get("research_brief"),
        "llm_structured_dossier": llm_dossier,
        "status": "enriched" if pages else "failed_no_data",
    }

    output = {
        "mode": "test_only",
        "external_platform_writes": False,
        "database_writes": False,
        "input": {
            "school_id": args.school_id,
            "school_name": name,
            "country": args.country or db_school.get("country"),
            "url": normalize_url(url),
            "max_pages": args.max_pages,
            "max_pages_per_section": args.max_pages_per_section,
            "max_news_pages": args.max_news_pages,
            "used_playwright_if_available": not args.requests_only,
            "browser_first": not args.requests_only and not args.http_first,
            "llm_enabled": not args.no_llm,
            "llm_model": None if args.no_llm else args.model,
        },
        "crawl": {
            "elapsed_seconds": round(time.time() - started, 2),
            "pages_scraped": len(pages),
            "source_urls": [page.url for page in pages],
            "latest_news_pages": [
                {"url": page.url, "title": page.title, "date_hint": page.date_hint}
                for page in pages
                if page.section_hint == "recent_news" or looks_like_news_index(page.url, page.section_hint)
            ],
            "errors": errors[:30],
        },
        "school_cache_record": school_cache_record,
        "deterministic_dossier": deterministic,
        "dossier": (llm_dossier or {}).get("dossier"),
        "llm_structured_dossier": llm_dossier,
        "llm_error": llm_error,
        "raw_pages": [asdict(page) for page in pages],
    }

    output_path = Path(args.output) if args.output else None
    if output_path is None:
        stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        output_path = DEFAULT_OUTPUT_DIR / f"{domain}_{stamp}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Saved dossier scrape test: {output_path}")
    print(f"Pages scraped: {len(pages)}")
    print(f"Deterministic quality: {deterministic['quality']['grade']} ({deterministic['quality']['score']})")
    if llm_error:
        print(f"LLM dossier failed but crawl was saved: {llm_error}")
    elif llm_dossier:
        print("LLM dossier: generated as structured JSON with plain-text dossier field")
    else:
        print("LLM dossier: skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
