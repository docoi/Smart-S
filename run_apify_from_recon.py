#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Full Scraper Runner (seed-only, sitemap-aware)
- DOES NOT follow links (MAX_DEPTH=0)
- Collects SOCIALS + links from the seed page(s)
- Parses SITEMAPS (if INCLUDE_SITEMAPS=1) and merges into caches
- Writes:
    cache_items_full.json
    site_social_links.json
    cache_urls_all.json
    cache_internal_urls.txt
    cache_external_urls.txt
    cache_social_urls.txt
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set
from urllib.parse import urlparse, urljoin

import requests

HERE = os.path.dirname(os.path.abspath(__file__))

# --- .env loader (override system env by default) ---
def load_env_file(path: str = ".env") -> None:
    try:
        p = Path(__file__).resolve().parent / path
        if not p.exists():
            return
        override = os.getenv("DOTENV_NO_OVERRIDE", "0").lower() not in ("1", "true", "yes")
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

load_env_file()

# --- Constants from env ---
APIFY_TOKEN = ((os.getenv("APIFY_TOKEN") or "").strip().strip('"').strip("'"))
APIFY_ACT_ID = (os.getenv("APIFY_ACT_ID") or "apify~web-scraper").strip()
APIFY_PROXY_GROUPS = os.getenv("APIFY_PROXY_GROUP") or os.getenv("APIFY_PROXY_GROUPS")

# Toggles
INCLUDE_SITEMAPS = os.getenv("INCLUDE_SITEMAPS", "0").lower() in ("1", "true", "yes")
COLLECT_LINKS    = os.getenv("COLLECT_LINKS",    "1").lower() in ("1", "true", "yes")

# Keep runs cheap
MAX_DEPTH    = 0      # do not follow links
REQUEST_RETRY = 1
PAGELOAD_TIMEOUT = 20


def _mask(tok: str) -> str:
    if not tok:
        return "<empty>"
    return tok[:10] + "..." + tok[-4:] if len(tok) > 18 else tok[:3] + "..." + tok[-3:]


def verify_apify_access(actor_id: str) -> None:
    """Preflight: show account; handle /v2/me vs /v2/users/me, fail on 401/403."""
    token = APIFY_TOKEN
    if not token:
        raise SystemExit("APIFY_TOKEN missing. Put a full-access token in .env and re-run.")

    def get_me(path: str):
        return requests.get(f"https://api.apify.com{path}", params={"token": token}, timeout=30)

    r = get_me("/v2/me")
    if r.status_code == 404:
        r = get_me("/v2/users/me")

    if r.status_code in (401, 403):
        print("[Apify] Preflight auth failed:", r.status_code, r.text[:300])
        raise SystemExit("APIFY_TOKEN invalid or lacks permissions. Use a full-access token.")

    if r.status_code == 404:
        print("[Apify] Preflight: /me endpoint not found (404). Continuing.")
        return

    try:
        body = r.json()
    except Exception:
        print("[Apify] Preflight JSON parse failed; continuing.")
        return

    me = body.get("data") or body
    print(f"[Apify] Auth OK as '{me.get('username','?')}'  token={_mask(token)}  actor={actor_id}")


def recon_profile(url: str) -> Dict[str, Any]:
    """Run local recon_actor.py if present; otherwise minimal defaults."""
    recon_path = os.path.join(HERE, "recon_actor.py")
    if os.path.exists(recon_path):
        try:
            import subprocess
            print("\n[Recon] Running…")
            p = subprocess.run([sys.executable, recon_path, url], cwd=HERE,
                               capture_output=True, text=True)
            stdout = (p.stdout or "").strip()
            first, last = stdout.find("{"), stdout.rfind("}")
            if p.returncode == 0 and first != -1 and last != -1 and last > first:
                blob = stdout[first:last+1]
                prof = json.loads(blob)
                print(json.dumps(prof, indent=2))
                return prof
        except Exception:
            pass

    prof = {
        "url": url,
        "hostname": urlparse(url).hostname,
        "recommendation": {
            "waitUntil": "domcontentloaded",
            "extraWaitMs": 0,
            "readinessSelector": "body",
            "reasons": []
        }
    }
    print("\n[Recon] (fallback defaults)")
    print(json.dumps(prof, indent=2))
    return prof


# --- Sitemaps ---
def parse_robots_for_sitemaps(base_url: str) -> List[str]:
    urls = []
    robots = urljoin(base_url, "/robots.txt")
    try:
        r = requests.get(robots, timeout=15)
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sm = line.split(":", 1)[1].strip()
                    if sm:
                        urls.append(sm)
    except Exception:
        pass
    return list(dict.fromkeys(urls))


def parse_sitemap_xml(xml_text: str) -> List[str]:
    """Return <loc> URLs from a sitemap or sitemap index."""
    locs: List[str] = []
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
        for loc in root.findall(".//{*}loc"):
            if loc.text:
                locs.append(loc.text.strip())
    except Exception:
        pass
    return locs


def same_host(u: str, host: str) -> bool:
    try:
        return urlparse(u).hostname == host
    except Exception:
        return False


def discover_sitemap_urls(base_url: str, cap_total: int = 5000) -> List[str]:
    """Fetch /robots.txt and common sitemaps, flatten to a de-duplicated URL list (same-host only)."""
    host = urlparse(base_url).hostname
    out: List[str] = []

    # Seed candidates
    candidates = [urljoin(base_url, "/sitemap.xml")]
    candidates.extend(parse_robots_for_sitemaps(base_url))
    candidates = list(dict.fromkeys(candidates))[:10]

    # Fetch sitemap or sitemap index, then flatten
    fetched: Set[str] = set()
    queue = list(candidates)
    try:
        while queue and len(out) < cap_total:
            sm_url = queue.pop(0)
            if sm_url in fetched:
                continue
            fetched.add(sm_url)
            try:
                r = requests.get(sm_url, timeout=20)
                if not (r.ok and "xml" in (r.headers.get("content-type","").lower())):
                    continue
                locs = parse_sitemap_xml(r.text)
                # If this looks like a sitemap index, push children; else, collect page URLs
                child_sitemaps = [u for u in locs if u.lower().endswith(".xml")]
                if child_sitemaps:
                    for u in child_sitemaps:
                        if u not in fetched and len(queue) < 50:
                            queue.append(u)
                page_urls = [u for u in locs if not u.lower().endswith(".xml")]
                out.extend(u for u in page_urls if same_host(u, host))
            except Exception:
                continue
    except Exception:
        pass

    # De-duplicate and cap
    out = list(dict.fromkeys(out))[:cap_total]
    return out


# --- Actor pageFunction ---
def build_page_function_js() -> str:
    # Minimal metadata + SOCIALS + link harvest from seed page(s) only
    return r"""
async function pageFunction(context) {
  const { request, jQuery, customData } = context;
  const $ = jQuery;
  const recon = (customData && customData.recon) || {};
  const collectLinks = !!(customData && customData.collectLinks);
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  // -- Small readiness wait
  if (recon.readinessSelector && context.waitForSelector) {
    try { await context.waitForSelector(recon.readinessSelector, { timeout: 8000 }); } catch (e) {}
  }
  if (recon.extraWaitMs && Number.isFinite(recon.extraWaitMs)) {
    await sleep(recon.extraWaitMs);
  }

  // --- Helpers ---
  const ABS = (u) => { try { return new URL(u, request.url).href; } catch(e) { return null; } };
  const SOCIAL_HOSTS = [
    "linkedin.com","facebook.com","instagram.com","x.com","twitter.com",
    "youtube.com","tiktok.com","threads.net","pinterest.com","glassdoor.com","glassdoor.co.uk"
  ];
  const platformOf = (url) => {
    if (!url) return "other";
    const u = url.toLowerCase();
    if (u.includes("linkedin.com"))  return "linkedin";
    if (u.includes("facebook.com"))  return "facebook";
    if (u.includes("instagram.com")) return "instagram";
    if (u.includes("twitter.com") || u.includes("x.com")) return "x";
    if (u.includes("youtube.com"))   return "youtube";
    if (u.includes("tiktok.com"))    return "tiktok";
    if (u.includes("threads.net"))   return "threads";
    if (u.includes("pinterest.com")) return "pinterest";
    if (u.includes("glassdoor."))    return "glassdoor";
    return "other";
  };
  const isLinkedInCompany = (url) => {
    const u = (url || "").toLowerCase();
    return u.includes("/company/") || u.includes("/school/") || u.includes("/showcase/");
  };

  // --- Socials from anchors
  const socialSet = new Set();
  document.querySelectorAll("a[href]").forEach(a => {
    const href = a.getAttribute("href");
    if (!href || /^(mailto:|tel:|javascript:)/i.test(href)) return;
    const abs = ABS(href);
    if (!abs) return;
    if (SOCIAL_HOSTS.some(h => abs.includes(h))) socialSet.add(abs);
  });

  // --- JSON-LD sameAs arrays
  try {
    const parseMaybe = (txt) => { try { return JSON.parse(txt); } catch(e) { return null; } };
    const flatten = (x) => Array.isArray(x) ? x : (x ? [x] : []);
    const rawBlocks = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
      .map(s => s.textContent).filter(Boolean);
    const ld = [];
    for (const raw of rawBlocks) {
      const val = parseMaybe(raw);
      if (!val) continue;
      if (Array.isArray(val)) ld.push(...val);
      else ld.push(val);
    }
    for (const obj of ld) {
      if (!obj || typeof obj !== "object") continue;
      const types = flatten(obj["@type"]).map(x => (typeof x === "string" ? x.toLowerCase() : ""));
      if (types.some(t => ["organization","localbusiness","website","corporation","project","brand"].includes(t))) {
        for (const u of flatten(obj["sameAs"]).filter(Boolean)) {
          const abs = ABS(u);
          if (abs && SOCIAL_HOSTS.some(h => abs.includes(h))) socialSet.add(abs);
        }
      }
    }
  } catch(e) {}

  // Classify socials
  const byPlatform = {};
  const allSocial = Array.from(socialSet);
  for (const u of allSocial) {
    const p = platformOf(u);
    (byPlatform[p] ||= []).push(u);
  }
  for (const p of Object.keys(byPlatform)) {
    byPlatform[p] = Array.from(new Set(byPlatform[p])).sort();
  }

  // LinkedIn best guess
  let linkedinCompany = null, linkedinAny = null;
  if (byPlatform.linkedin && byPlatform.linkedin.length) {
    linkedinAny = byPlatform.linkedin[0];
    const companyHits = byPlatform.linkedin.filter(isLinkedInCompany);
    if (companyHits.length) linkedinCompany = companyHits[0];
  }

  // --- Optional link harvesting (no enqueuing/navigation)
  const links = { internal: [], external: [], social: [] };
  if (collectLinks) {
    const seen = new Set();
    const host = new URL(request.url).hostname;

    const push = (u) => {
      if (!u || seen.has(u)) return;
      seen.add(u);
      try {
        const h = new URL(u).hostname;
        if (SOCIAL_HOSTS.some(s => u.includes(s))) links.social.push(u);
        else if (h === host) links.internal.push(u);
        else links.external.push(u);
      } catch(e) {}
    };

    document.querySelectorAll("a[href]").forEach(a => {
      const href = a.getAttribute("href");
      if (!href || /^(mailto:|tel:|javascript:)/i.test(href)) return;
      const abs = ABS(href);
      if (abs) push(abs);
    });

    document.querySelectorAll("[data-href],[data-link]").forEach(el => {
      const h = el.getAttribute("data-href") || el.getAttribute("data-link");
      const abs = ABS(h);
      if (abs) push(abs);
    });

    document.querySelectorAll("link[rel=canonical][href], link[rel=next][href], link[rel=prev][href]").forEach(el => {
      const h = el.getAttribute("href");
      const abs = ABS(h);
      if (abs) push(abs);
    });

    links.internal = Array.from(new Set(links.internal)).slice(0, 3000);
    links.external = Array.from(new Set(links.external)).slice(0, 1500);
    links.social   = Array.from(new Set(links.social)).slice(0, 500);
  }

  // Minimal page metadata
  const title = $('title').text().trim();
  const heading = $('h1').first().text().trim()
              || $('h2').first().text().trim()
              || $('[class*=title],[class*=headline],[role=heading]').first().text().trim();

  return {
    url: request.url,
    title,
    heading,
    textLen: $('body').text().trim().length,
    social: {
      by_platform: byPlatform,
      all: allSocial.sort(),
      linkedin_company: linkedinCompany,
      linkedin_any: linkedinAny
    },
    links
  };
}
"""


def build_input_from_profile(profile: Dict[str, Any], collect_links: bool, max_requests: int) -> Dict[str, Any]:
    seeds = [{"url": profile["url"]}]

    recon = profile.get("recommendation") or {}
    wait_until = recon.get("waitUntil") or "domcontentloaded"
    extra_wait = recon.get("extraWaitMs") or 0
    readiness = recon.get("readinessSelector") or "body"

    input_payload: Dict[str, Any] = {
        "startUrls": seeds,
        "proxyConfiguration": {"useApifyProxy": True},
        "injectJQuery": True,
        "maxRequestsPerCrawl": max_requests,
        "maxDepth": MAX_DEPTH,
        "pageLoadTimeoutSecs": PAGELOAD_TIMEOUT,
        "maxRequestRetries": REQUEST_RETRY,
        "customData": {
            "recon": {
                "waitUntil": wait_until,
                "extraWaitMs": extra_wait,
                "readinessSelector": readiness,
                "reasons": recon.get("reasons", []),
            },
            "collectLinks": bool(collect_links),
        },
        "pageFunction": build_page_function_js(),
        "waitUntil": [wait_until],
        "useChrome": True,
        "useStealth": True,
    }

    if APIFY_PROXY_GROUPS:
        input_payload["proxyConfiguration"]["apifyProxyGroups"] = [APIFY_PROXY_GROUPS]

    print(f"\n[Full scraper] Building input from recon… (COLLECT_LINKS={'ON' if collect_links else 'OFF'}, INCLUDE_SITEMAPS={'ON' if INCLUDE_SITEMAPS else 'OFF'})")
    print(json.dumps(input_payload, indent=2))
    return input_payload


def start_run(ws_input: Dict[str, Any]) -> Dict[str, Any]:
    def try_start(actor_id: str) -> requests.Response:
        url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
        headers = {"Content-Type": "application/json"}
        params = {}
        if APIFY_TOKEN:
            headers["Authorization"] = f"Bearer {APIFY_TOKEN}"
            params["token"] = APIFY_TOKEN
        return requests.post(url, json=ws_input, headers=headers, params=params, timeout=180)

    r = try_start(APIFY_ACT_ID)
    if r.status_code in (401, 403):
        print(f"[Apify ERROR] {r.status_code} when starting actor '{APIFY_ACT_ID}'.")
        try: print("[Apify ERROR] Body:", r.text[:600])
        except Exception: pass
        if APIFY_ACT_ID.lower() != "apify~web-scraper":
            print("[Apify] Retrying with public actor 'apify~web-scraper' ...")
            r = try_start("apify~web-scraper")

    if r.status_code >= 400:
        body = r.text[:1000]
        print(f"[Apify ERROR] {r.status_code} when starting actor.")
        print("[Apify ERROR] Body:", body)
        if "Monthly usage hard limit exceeded" in body:
            raise SystemExit(
                "Apify blocked the run: Monthly usage hard limit exceeded.\n"
                "- Raise the limit in Apify Console → Billing → Usage & limits, or\n"
                "- Switch APIFY_TOKEN in .env to a workspace with headroom."
            )
        r.raise_for_status()

    return r.json().get("data") or r.json()


def wait_for_finish(run_id: str, timeout_sec: int = 900, poll_secs: int = 3) -> Dict[str, Any]:
    base = f"https://api.apify.com/v2/actor-runs/{run_id}"
    start = time.time()
    last = None

    console_url = f"https://console.apify.com/actors/runs/{run_id}"
    log_url = f"{base}/log?token={APIFY_TOKEN}&stream=true"
    print(f"\n[Apify] Live run : {console_url}")
    print(f"[Apify] Live log : {log_url}\n")

    while True:
        r = requests.get(f"{base}?token={APIFY_TOKEN}", timeout=60)
        r.raise_for_status()
        data = r.json()["data"]
        status = data.get("status", "UNKNOWN")

        if status != last:
            print(f"[Apify] Status → {status}")
            last = status

        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            return data

        if time.time() - start > timeout_sec:
            print("[Apify] Wait timeout reached — returning latest status.")
            return data

        time.sleep(poll_secs)


def fetch_dataset_items(dataset_id: str, limit_per_page: int = 1000, clean: bool = True) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "token": APIFY_TOKEN,
            "format": "json",
            "clean": "true" if clean else "false",
            "offset": offset,
            "limit": limit_per_page,
        }
        url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        try:
            batch = r.json()
        except ValueError:
            batch = []
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit_per_page:
            break
        offset += len(batch)
    return items


def save_caches(items: List[Dict[str, Any]], sitemap_urls: List[str], base_url: str) -> None:
    # Always save raw items
    with open(os.path.join(HERE, "cache_items_full.json"), "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    host = urlparse(base_url).hostname or ""

    # --- Collate socials across items (always) ---
    by_platform: Dict[str, Set[str]] = {}
    flat: Set[str] = set()

    def push_soc(p: str, u: str):
        if not p or not u:
            return
        by_platform.setdefault(p, set()).add(u)
        flat.add(u)

    for it in items:
        soc = (it or {}).get("social") or {}
        bp = soc.get("by_platform") or {}
        if isinstance(bp, dict):
            for p, urls in bp.items():
                if isinstance(urls, list):
                    for u in urls:
                        push_soc(p, u)
        # legacy fields resilience
        for key in ("linkedin","facebook","instagram","x","twitter","youtube","tiktok","threads","pinterest","glassdoor"):
            val = soc.get(key)
            if isinstance(val, str):
                push_soc(key, val)
            elif isinstance(val, list):
                for u in val:
                    push_soc(key, u)

    social_out = {
        "by_platform": {k: sorted(list(v)) for k, v in by_platform.items()},
        "all": sorted(list(flat)),
    }
    # Best company LinkedIn if any item had it
    ln = social_out["by_platform"].get("linkedin", [])
    for it in items:
        cand = (((it or {}).get("social") or {}).get("linkedin_company")) or None
        if cand and cand not in ln:
            ln.insert(0, cand)
    if ln:
        social_out["linkedin_company"] = ln[0]

    with open(os.path.join(HERE, "site_social_links.json"), "w", encoding="utf-8") as f:
        json.dump(social_out, f, ensure_ascii=False, indent=2)

    # --- Collate LINKS: from seed pages (items) + from sitemaps ---
    internal: Set[str] = set()
    external: Set[str] = set()
    social:   Set[str] = set()

    # From items (seed pages)
    for it in items:
        links = (it or {}).get("links") or {}
        for u in (links.get("internal") or []): internal.add(u)
        for u in (links.get("external") or []): external.add(u)
        for u in (links.get("social")   or []): social.add(u)

    # From sitemaps (merge without visiting)
    for u in sitemap_urls:
        try:
            if not isinstance(u, str):
                continue
            if urlparse(u).hostname == host:
                internal.add(u)
            else:
                external.add(u)
        except Exception:
            continue

    urls_all = {
        "internal": sorted(internal),
        "external": sorted(external),
        "social":   sorted(social),
    }
    with open(os.path.join(HERE, "cache_urls_all.json"), "w", encoding="utf-8") as f:
        json.dump(urls_all, f, ensure_ascii=False, indent=2)

    def write_list(path: str, seq: List[str]) -> None:
        with open(os.path.join(HERE, path), "w", encoding="utf-8") as f:
            for u in seq:
                f.write(u + "\n")

    write_list("cache_internal_urls.txt", urls_all["internal"])
    write_list("cache_external_urls.txt", urls_all["external"])
    write_list("cache_social_urls.txt",   urls_all["social"])

    print("\n[Cache saved]")
    print("- cache_items_full.json")
    print("- site_social_links.json")
    print("- cache_urls_all.json")
    print("- cache_internal_urls.txt")
    print("- cache_external_urls.txt")
    print("- cache_social_urls.txt")


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_apify_from_recon.py <url>")
        sys.exit(2)

    url = sys.argv[1].strip()

    profile = recon_profile(url)

    # Discover sitemap URLs (cheap; not visited)
    sitemap_urls: List[str] = []
    if INCLUDE_SITEMAPS:
        print("\n[Sitemaps] Discovering…")
        sitemap_urls = discover_sitemap_urls(profile["url"], cap_total=5000)
        print(f"[Sitemaps] Collected {len(sitemap_urls)} URL(s) from sitemaps.")

    # Build input and run
    max_requests = max(1, len((profile.get("startUrls") or [])) or 1)
    ws_input = build_input_from_profile(profile, collect_links=COLLECT_LINKS, max_requests=max_requests)

    print("\n[Full scraper] Starting run…")
    verify_apify_access(APIFY_ACT_ID)

    run_data = start_run(ws_input)
    final = wait_for_finish(run_data["id"], timeout_sec=900)
    status = final.get("status")
    print(f"\n[Full scraper] Status: {status}")

    dataset_id = final.get("defaultDatasetId")
    items: List[Dict[str, Any]] = []
    if status == "SUCCEEDED" and dataset_id:
        items = fetch_dataset_items(dataset_id)
        print("\n[Full scraper] Items preview:")
        print(json.dumps(items[:2], indent=2))
    else:
        print("\n[Full scraper] No dataset returned or run did not succeed (still saving sitemap URLs & empty socials).")

    # Save caches (merge items + sitemaps)
    save_caches(items, sitemap_urls, base_url=profile["url"])


if __name__ == "__main__":
    main()
