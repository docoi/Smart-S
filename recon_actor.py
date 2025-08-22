#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
recon_actor.py
- Local, fast site reconnaissance with Playwright.
- No Apify calls here (tokens/actors not used).
- Outputs a JSON profile with timings, JS/SPA hints, infinite scroll signals,
  and a scraping recommendation.
"""

import sys
import json
import asyncio
import hashlib
from urllib.parse import urlparse

from playwright.async_api import async_playwright

# --- .env loader (override system env by default; kept for consistency) ---
def _load_env_file(path: str = ".env") -> None:
    try:
        import os, pathlib
        p = pathlib.Path(__file__).resolve().parent / path
        if not p.exists():
            return
        override = (os.getenv("DOTENV_NO_OVERRIDE", "0").lower() not in ("1", "true", "yes"))
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if override or k not in os.environ:
                os.environ[k] = v
    except Exception:
        pass

_load_env_file()  # (not used by this file, but keeps behavior consistent)

# ---------------------------------------------------------------------------

def _hash_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


async def _get_nav_timings(page):
    """Use Navigation Timing L2 where available."""
    data = await page.evaluate(
        "() => {"
        "  const nav = performance.getEntriesByType('navigation')[0];"
        "  if (!nav) return null;"
        "  return {"
        "    domContentLoaded: nav.domContentLoadedEventEnd,"
        "    loadEventEnd: nav.loadEventEnd,"
        "    responseEnd: nav.responseEnd,"
        "    ttfb: nav.responseStart - nav.requestStart"
        "  };"
        "}"
    )
    return data or {}


async def _sniff_frameworks(page):
    """Heuristics for common SPA frameworks."""
    return await page.evaluate(
        "() => ({"
        "  react: !!window.__REACT_DEVTOOLS_GLOBAL_HOOK__ || !!document.querySelector('[data-reactroot]'),"
        "  next: !!window.__NEXT_DATA__,"
        "  vue: !!(window.__VUE__ || window.__VUE_DEVTOOLS_GLOBAL_HOOK__ || document.querySelector('[data-v-app]')),"
        "  angular: !!document.querySelector('[ng-version]') || !!window.ng,"
        "  svelte: !!document.querySelector('[data-svelte-h]')"
        "})"
    )


async def _readiness_selector(page):
    """Pick a stable, content-heavy selector to gate readiness."""
    candidates = ["main", "article", "#content", ".content", "#__next", "#root", "#app", "body"]
    found = []
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el:
                txt = (await el.inner_text()) or ""
                found.append((sel, len(txt.strip())))
        except Exception:
            pass
    if not found:
        return {"candidates": [], "best": None}
    found.sort(key=lambda t: t[1], reverse=True)
    return {"candidates": [f for f, _ in found], "best": found[0][0]}


async def _dom_churn_probe(page):
    """Sample body text hash several times to see if content keeps changing."""
    samples = []
    for _ in range(4):
        try:
            txt = await page.evaluate("() => document.body ? document.body.innerText : ''")
        except Exception:
            txt = ""
        samples.append(_hash_text(txt[:20000]))
        await page.wait_for_timeout(600)
    churn = any(samples[i] != samples[i - 1] for i in range(1, len(samples)))
    return {"churn": churn, "samples": samples}


async def _infinite_scroll_probe(page):
    """
    Scroll a few times; see if document grows.
    Also collect which selectors/classes increased so we can suggest an item selector.
    """
    before_height = await page.evaluate("() => document.documentElement.scrollHeight")

    selector_candidates = [
        "article", "li", ".card", ".product", ".post", ".item", ".listing", ".result", ".entry",
        ".tile", ".news", ".job", ".media", ".module", ".component", ".team", ".person", ".member",
        ".grid > *", ".row > *", ".col"
    ]

    def js_count_block(selectors):
        return (
            "(sels => {"
            "  const out = {}; "
            "  for (const s of sels) { try { out[s] = document.querySelectorAll(s).length; } catch(e) { out[s] = 0; } }"
            "  return out;"
            "})"
        )

    before_counts = await page.evaluate(js_count_block(selector_candidates), selector_candidates)

    for _ in range(3):
        await page.evaluate("() => window.scrollTo(0, document.documentElement.scrollHeight)")
        await page.wait_for_timeout(1000)

    after_height = await page.evaluate("() => document.documentElement.scrollHeight")
    after_counts = await page.evaluate(js_count_block(selector_candidates), selector_candidates)

    grows = after_height > before_height + 200
    increased = []
    for sel in selector_candidates:
        b = int(before_counts.get(sel, 0) or 0)
        a = int(after_counts.get(sel, 0) or 0)
        if a > b and (a - b) >= 3:
            increased.append((sel, a - b))

    increased.sort(key=lambda t: t[1], reverse=True)
    recommended_item_selector = increased[0][0] if increased else None

    return {
        "before": before_height,
        "after": after_height,
        "grows": grows,
        "increasedSelectors": increased,
        "recommendedItemSelector": recommended_item_selector,
    }


async def _static_vs_rendered(page, response_text):
    """Compare raw HTML vs rendered DOM text length (simple heuristic)."""
    static_len = len((response_text or "").strip())
    rendered_len = await page.evaluate(
        "() => {"
        "  const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_TEXT);"
        "  let len = 0, n;"
        "  while ((n = walker.nextNode())) {"
        "    const t = (n.nodeValue || '').trim();"
        "    if (t) len += t.length;"
        "  }"
        "  return len;"
        "}"
    )
    ratio = (rendered_len / static_len) if static_len else (10 if rendered_len else 1)
    js_required = ratio > 1.8 or (static_len == 0 and rendered_len > 0)
    return {"staticTextLen": static_len, "renderedTextLen": rendered_len, "ratio": round(ratio, 2), "jsRequired": js_required}


async def run_recon(url: str) -> dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        profile = {
            "url": url,
            "hostname": urlparse(url).hostname,
            "frameworks": {},
            "js_required": None,
            "timings": {},
            "readinessSelector": None,
            "infiniteScroll": None,
            "dynamicSignals": {},
            "recommendation": {},
        }

        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(300)

            timings = await _get_nav_timings(page)
            profile["timings"] = timings or {}

            frameworks = await _sniff_frameworks(page)
            frameworks["any"] = any(frameworks.values())
            profile["frameworks"] = frameworks

            raw_html = await (resp.text() if resp else page.content())
            svr = await _static_vs_rendered(page, raw_html)
            profile["js_required"] = svr["jsRequired"]
            profile["dynamicSignals"] = {"staticVsRendered": svr}

            churn = await _dom_churn_probe(page)
            profile["dynamicSignals"]["domChurn"] = churn

            ready = await _readiness_selector(page)
            profile["readinessSelector"] = ready.get("best")

            inf = await _infinite_scroll_probe(page)
            profile["infiniteScroll"] = inf

            reasons = []
            wait_until = "domcontentloaded"
            extra_wait = 0

            if frameworks["any"] or profile["js_required"]:
                wait_until = "networkidle2"
                reasons.append("SPA/JS rendering detected")

            if (timings.get("loadEventEnd") or 0) > 3000:
                if not (frameworks["any"] or profile["js_required"]):
                    wait_until = "load"
                reasons.append("Slow load detected")

            if churn["churn"]:
                extra_wait = max(500, min(2000, int((timings.get("loadEventEnd") or 1200) * 0.25)))
                reasons.append("Post-load DOM changes detected")

            recommendation = {
                "waitUntil": wait_until,
                "extraWaitMs": extra_wait,
                "infiniteScroll": bool(inf["grows"]),
                "readinessSelector": profile["readinessSelector"],
                "recommendedItemSelector": inf.get("recommendedItemSelector"),
                "reasons": reasons,
            }
            profile["recommendation"] = recommendation

        except Exception as e:
            profile["error"] = str(e)

        await browser.close()
        return profile


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python recon_actor.py <url>")
        sys.exit(1)
    target = sys.argv[1]
    out = asyncio.run(run_recon(target))
    print(json.dumps(out, indent=2))
