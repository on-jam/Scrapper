"""Offline regression tests for crawl ownership, partial evidence, and budgets."""
import asyncio
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import http_client
import scraper
import sessions


class CrawlTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.session = Mock(cookies={})
        self.stack.enter_context(patch.object(scraper, "make_session", return_value=self.session))
        self.stack.enter_context(patch.object(scraper, "load_cookies"))
        self.save = self.stack.enter_context(patch.object(scraper, "save_cookies"))
        self.stack.enter_context(patch.object(scraper, "discover_robots", return_value=(0, [])))
        self.stack.enter_context(patch.object(scraper, "discover_sitemap_urls", return_value=[]))
        self.urls = ["https://school.test/a", "https://school.test/b", "https://school.test/c"]
        self.stack.enter_context(patch.object(scraper, "build_candidate_urls", return_value=self.urls.copy()))
        self.responses = []
        self.fetch = self.stack.enter_context(patch.object(scraper, "fetch", side_effect=self.response))
        self.stack.enter_context(patch.object(scraper, "flaresolverr", side_effect=AssertionError("unexpected network fallback")))

    def response(self, session, url, *args):
        response = Mock(status_code=200, headers={"content-type": "text/html"},
                        text="<title>School</title><p>" + "learning " * 150 + "</p>")
        self.responses.append(response)
        return response

    def crawl(self, **kwargs):
        options = dict(base_url="https://school.test", max_pages=3, timeout=20,
                       use_playwright=False, max_pdf_pages=6,
                       max_pages_per_section=None, max_news_pages=None, browser_first=False)
        options.update(kwargs)
        return scraper.scrape_site(**options)

    def test_parse_failure_keeps_prior_and_later_pages_and_closes_resources(self):
        original = scraper.parse_html
        def parse(html, url):
            if url.endswith("/b"):
                raise ValueError("broken page")
            return original(html, url)
        with patch.object(scraper, "parse_html", side_effect=parse):
            pages, errors = self.crawl()
        self.assertEqual([p.url for p in pages], [self.urls[0], self.urls[2]])
        self.assertIn("broken page", errors[0])
        self.session.close.assert_called_once()
        self.save.assert_called_once()
        for response in self.responses:
            response.close.assert_called_once()

    def test_discovery_exception_still_closes_session_and_renderer(self):
        renderer = Mock()
        scraper._RENDERER_LOCAL.renderer = renderer
        with patch.object(scraper, "discover_robots", side_effect=RuntimeError("discovery failed")):
            pages, errors = self.crawl()
        self.assertEqual(pages, [])
        self.assertIn("discovery failed", errors[0])
        self.session.close.assert_called_once()
        renderer.close.assert_called_once()
        self.assertFalse(hasattr(scraper._RENDERER_LOCAL, "renderer"))

    def test_save_failure_does_not_prevent_close(self):
        self.save.side_effect = OSError("disk full")
        pages, _ = self.crawl()
        self.assertEqual(len(pages), 3)
        self.session.close.assert_called_once()

    def test_metadata_failure_keeps_text_and_navigation(self):
        with patch.object(scraper, "extract_structured_data", side_effect=ValueError("metadata")):
            pages, errors = self.crawl()
        self.assertEqual(len(pages), 3)
        self.assertEqual(len(errors), 3)

    def test_sparse_navigation_still_discovers_dynamic_pages(self):
        with patch.object(scraper, "build_candidate_urls", return_value=[self.urls[0]]):
            def response(session, url, *args):
                result = self.response(session, url, *args)
                if url == self.urls[0]:
                    result.text = '<button data-href="/b">Explore</button>'
                return result
            self.fetch.side_effect = response
            pages, errors = self.crawl()
        self.assertEqual([p.url for p in pages], [self.urls[1]])
        self.assertEqual(errors, [])

    def test_deadline_keeps_partial_results_and_prevents_next_fetch(self):
        now = [0.0]
        original = scraper.parse_html
        def parse(html, url):
            result = original(html, url)
            now[0] = 11.0
            return result
        with patch.object(scraper.time, "monotonic", side_effect=lambda: now[0]), patch.object(scraper, "parse_html", side_effect=parse):
            pages, errors = self.crawl(crawl_timeout=10)
        self.assertEqual(len(pages), 1)
        self.fetch.assert_called_once()
        self.assertIn("overall crawl deadline exceeded", errors[0])
        self.assertIsNone(getattr(http_client.CRAWL_STATE, "deadline", None))
        self.session.close.assert_called_once()

    def test_rendered_parse_failure_preserves_http_evidence(self):
        self.fetch.side_effect = lambda *args: Mock(status_code=200, headers={},
                                                   text="learning " * 60)
        original = scraper.parse_html
        def parse(html, url):
            if html == "bad rendered":
                raise ValueError("bad rendered")
            return original(html, url)
        with patch.object(scraper, "render_page", return_value="bad rendered"), patch.object(scraper, "parse_html", side_effect=parse):
            pages, errors = self.crawl(use_playwright=True)
        self.assertEqual(len(pages), 3)
        self.assertEqual(len(errors), 3)


class RendererTests(unittest.TestCase):
    def setUp(self):
        self.contexts = []
        self.paths = []
        async def launch(path, **options):
            self.paths.append(path)
            context = SimpleNamespace(
                close=AsyncMock(), set_extra_http_headers=AsyncMock(),
                add_init_script=AsyncMock(), new_page=AsyncMock())
            self.contexts.append(context)
            return context
        self.playwright = SimpleNamespace(
            chromium=SimpleNamespace(launch_persistent_context=AsyncMock(side_effect=launch)),
            stop=AsyncMock())
        api = SimpleNamespace(async_playwright=lambda: SimpleNamespace(start=AsyncMock(return_value=self.playwright)))
        self.modules = patch.dict(sys.modules, {"playwright.async_api": api})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.renderers = []
        self.addCleanup(self.close_renderers)

    def renderer(self):
        renderer = scraper._ThreadPlaywrightRenderer()
        self.renderers.append(renderer)
        return renderer

    def close_renderers(self):
        for renderer in self.renderers:
            if not renderer.loop.is_closed():
                renderer.close()

    def test_domains_are_bounded_and_same_domain_reuses_context(self):
        renderer = self.renderer()
        first = renderer.loop.run_until_complete(renderer._context_for("https://a.test"))
        again = renderer.loop.run_until_complete(renderer._context_for("https://a.test/page"))
        self.assertIs(first, again)
        for index in range(12):
            renderer.loop.run_until_complete(renderer._context_for(f"https://school{index}.test"))
            self.assertEqual(len(renderer.contexts), 1)
        first.close.assert_awaited_once()
        renderer.close()
        self.assertTrue(renderer.loop.is_closed())
        self.playwright.stop.assert_awaited_once()
        self.assertTrue(all(not Path(path).exists() for path in self.paths))

    def test_same_host_uses_exclusive_profiles(self):
        for _ in range(2):
            renderer = self.renderer()
            renderer.loop.run_until_complete(renderer._context_for("https://same.test"))
        self.assertNotEqual(self.paths[0], self.paths[1])

    def test_setup_failure_closes_context_and_driver(self):
        renderer = self.renderer()
        original = self.playwright.chromium.launch_persistent_context.side_effect
        async def broken(*args, **kwargs):
            context = await original(*args, **kwargs)
            context.add_init_script.side_effect = RuntimeError("setup")
            return context
        self.playwright.chromium.launch_persistent_context.side_effect = broken
        self.assertIsNone(renderer.render("https://school.test", 20))
        self.contexts[0].close.assert_awaited_once()
        self.playwright.stop.assert_awaited_once()
        self.assertEqual(renderer.contexts, {})

    def test_navigation_failure_closes_page_and_context(self):
        renderer = self.renderer()
        context = renderer.loop.run_until_complete(renderer._context_for("https://school.test"))
        page = SimpleNamespace(goto=AsyncMock(side_effect=RuntimeError("navigation")), close=AsyncMock())
        context.new_page.return_value = page
        self.assertIsNone(renderer.render("https://school.test", 20))
        page.close.assert_awaited_once()
        context.close.assert_awaited_once()

    def test_browser_budget_cancels_hung_navigation(self):
        renderer = self.renderer()
        context = renderer.loop.run_until_complete(renderer._context_for("https://school.test"))
        async def never(*args, **kwargs):
            await asyncio.Event().wait()
        page = SimpleNamespace(goto=AsyncMock(side_effect=never), close=AsyncMock())
        context.new_page.return_value = page
        self.assertIsNone(renderer.render("https://school.test", 0.01))
        page.close.assert_awaited_once()
        context.close.assert_awaited_once()


class GeminiBoundaryTests(unittest.TestCase):
    def test_invalid_roots_and_dossiers_are_rejected(self):
        for text in ('[]', '[{"dossier":"text"}]', 'null', '42', '"text"',
                     '{"dossier":[]}', '{"dossier":{}}', '{"dossier":null}', '{"dossier":" "}'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                scraper.parse_json_object(text)
        self.assertEqual(scraper.parse_json_object('{"dossier":"evidence"}'), {"dossier": "evidence"})
        self.assertEqual(scraper.parse_json_object('preface {"dossier":"evidence"}'), {"dossier": "evidence"})

    def test_gemini_http_error_has_no_query_key_or_request(self):
        import requests
        import traceback
        response = Mock(status_code=400)
        response.raise_for_status.side_effect = requests.HTTPError(
            "400 for url: https://example.test?key=synthetic-test-token")
        with patch.object(scraper, "gemini_key", return_value="synthetic-test-token"), patch.object(scraper, "request_with_cooldown", return_value=response), patch.object(scraper.time, "sleep"):
            try:
                scraper.call_gemini(None, "https://school.test", [], {}, "test", 100)
            except requests.HTTPError as error:
                self.assertNotIn("synthetic-test-token", traceback.format_exc())
                self.assertIsNone(error.request)
                self.assertIsNone(error.response)
                self.assertEqual(str(error), "Gemini HTTP 400")
            else:
                self.fail("HTTP failure should propagate safely")
        response.close.assert_called_once()

    def test_gemini_transport_error_is_sanitized(self):
        import requests
        with patch.object(scraper, "gemini_key", return_value="synthetic-test-token"), patch.object(scraper, "request_with_cooldown", side_effect=requests.ConnectionError("url?key=synthetic-test-token")), patch.object(scraper, "GEMINI_MAX_RETRIES", 0), patch.object(scraper.time, "sleep"):
            with self.assertRaisesRegex(requests.RequestException, "^Gemini transport request failed$"):
                scraper.call_gemini(None, "https://school.test", [], {}, "test", 100)


class TransportAndCookieTests(unittest.TestCase):
    def test_fetch_passes_remaining_budget_to_cooldown(self):
        self.addCleanup(setattr, http_client.CRAWL_STATE, "deadline", None)
        http_client.CRAWL_STATE.deadline = 12
        response = Mock()
        with patch.object(http_client.time, "monotonic", return_value=10), patch.object(scraper, "request_with_cooldown", return_value=response) as cooldown:
            self.assertIs(scraper.fetch(Mock(), "https://school.test", 20), response)
        self.assertEqual(cooldown.call_args.kwargs["max_wait_seconds"], 2)

    def test_http_timeout_uses_remaining_budget(self):
        self.addCleanup(setattr, http_client.CRAWL_STATE, "deadline", None)
        http_client.CRAWL_STATE.deadline = 12
        session = Mock()
        with patch.object(http_client.time, "monotonic", return_value=10):
            http_client.request(session, "GET", "https://school.test", 20)
        self.assertEqual(session.request.call_args.kwargs["timeout"], (2, 2))
        with patch.object(http_client.time, "monotonic", return_value=12):
            with self.assertRaises(http_client.CrawlDeadlineExceeded):
                http_client.request(session, "GET", "https://school.test", 20)
        self.assertEqual(session.request.call_count, 1)

    def test_deadlines_are_isolated_across_six_workers(self):
        barrier = threading.Barrier(6)
        def run(index):
            http_client.CRAWL_STATE.deadline = 100 + index
            barrier.wait(timeout=5)
            result = http_client.remaining_timeout(1000)
            http_client.CRAWL_STATE.deadline = None
            return result
        with patch.object(http_client.time, "monotonic", return_value=90), ThreadPoolExecutor(max_workers=6) as pool:
            self.assertEqual(list(pool.map(run, range(6))), list(range(10, 16)))

    def test_concurrent_cookie_writes_are_complete_and_leave_no_temps(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(sessions, "CACHE", Path(directory)):
            def save(index):
                sessions.save_cookies(SimpleNamespace(cookies={"value": str(index)}), "school.test")
            with ThreadPoolExecutor(max_workers=6) as pool:
                list(pool.map(save, range(30)))
            target = SimpleNamespace(cookies={})
            sessions.load_cookies(target, "school.test")
            self.assertIn(target.cookies["value"], [str(i) for i in range(30)])
            self.assertEqual([p.name for p in Path(directory).iterdir()], ["school.test.pkl"])

    def test_cookie_directory_failure_is_nonfatal(self):
        with patch.object(Path, "mkdir", side_effect=OSError("disk")):
            sessions.save_cookies(SimpleNamespace(cookies={}), "school.test")

    def test_discovery_responses_close_and_deadlines_propagate(self):
        response = Mock(status_code=200, text="Sitemap: https://school.test/sitemap.xml")
        with patch.object(scraper, "fetch", return_value=response):
            scraper.discover_robots(Mock(), "https://school.test", 20)
        response.close.assert_called_once()
        with patch.object(scraper, "fetch", side_effect=http_client.CrawlDeadlineExceeded("deadline")):
            with self.assertRaises(http_client.CrawlDeadlineExceeded):
                scraper.discover_sitemap_urls(Mock(), "https://school.test", 20)


if __name__ == "__main__":
    unittest.main()
