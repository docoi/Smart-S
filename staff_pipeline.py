#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
End-to-end Staff Scraper Pipeline Orchestrator

Runs:
  1) recon_actor.py
  2) run_apify_from_recon.py
  3) select_and_scrape_staff.py

Usage:
  python staff_pipeline.py crewsaders.com
  python staff_pipeline.py www.crewsaders.com
  python staff_pipeline.py https://www.crewsaders.com

Options:
  --include-sitemaps   Opt-in to seeding the crawler with sitemap URLs (slower).
  --skip-recon         Skip recon step.
  --skip-crawl         Skip crawl step.
  --skip-staff         Skip staff scrape step.
  --no-force-www       Do not force 'www.' on apex domains.
  --no-force-https     Do not force https scheme.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse, urlunparse

HERE = os.path.dirname(os.path.abspath(__file__))

# --- .env loader (override system env by default) ---

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
        # don't hard-crash if .env can't be read
        pass

load_env_file()  # load as early as possible



# --- .env loader (no dependency on python-dotenv) ---
def load_env_file(path: str = ".env") -> None:
    try:
        p = path if os.path.isabs(path) else os.path.join(HERE, path)
        if not os.path.exists(p):
            return
        with open(p, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k, v)  # do not overwrite if already set
    except Exception:
        pass

load_env_file()  # load as early as possible

SCRIPTS: List[Tuple[str, str]] = [
    ("Recon", os.path.join(HERE, "recon_actor.py")),
    ("Crawl", os.path.join(HERE, "run_apify_from_recon.py")),
    ("Staff", os.path.join(HERE, "select_and_scrape_staff.py")),
]

STAFF_RESULTS_JSON = os.path.join(HERE, "staff_scrape_results.json")


def canonicalize_url(raw: str, force_www: bool = True, force_https: bool = True) -> str:
    """Normalize user input into a canonical homepage URL.

    Rules:
      - add https:// if missing (force_https=True)
      - if host is apex like 'example.com' and force_www=True → add 'www.' → 'www.example.com'
      - leave subdomains (e.g., 'app.example.com') alone
      - drop path/query/fragment
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty URL")

    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", s):
        s = "https://" + s

    p = urlparse(s)
    scheme = "https" if force_https else (p.scheme or "https")
    host = (p.hostname or "").lower()

    if host.endswith("."):
        host = host[:-1]
    if not host:
        raise ValueError(f"Invalid URL: {raw}")

    is_ip = re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", host) is not None

    if force_www and not is_ip and not host.startswith("www.") and host.count(".") == 1:
        host = "www." + host

    netloc = host
    if p.port:
        netloc = f"{host}:{p.port}"

    canon = urlunparse((scheme, netloc, "/", "", "", ""))
    return canon.rstrip("/")


def run_script(step_name: str, script_path: str, url: str, env: Dict[str, str]) -> None:
    if not os.path.exists(script_path):
        raise SystemExit(f"[{step_name}] Not found: {script_path}")

    print(f"\n[{step_name}] ➜ {os.path.basename(script_path)} {url}")
    t0 = time.monotonic()
    proc = subprocess.run([sys.executable, script_path, url], cwd=HERE, env=env)
    dt = time.monotonic() - t0
    print(f"[{step_name}] Exit code {proc.returncode} ({dt:.1f}s)")

    if proc.returncode != 0:
        raise SystemExit(f"[{step_name}] failed with exit code {proc.returncode}")


def summarize_results(path: str) -> None:
    import os, json, re
    if not os.path.exists(path):
        print(f"[Summary] {path} not found.")
        return

    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f"[Summary] Failed to parse {path}: {e}")
            return

    # Normalize dataset shape
    items = data if isinstance(data, list) else (data.get("items") or [])
    if not isinstance(items, list):
        items = [items]

    def norm(v):
        """Return a clean string for any value type."""
        if v is None:
            return ""
        if isinstance(v, str):
            return v.strip()
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, list):
            return " ".join([norm(x) for x in v if norm(x)])
        if isinstance(v, dict):
            # try common text keys first
            for k in ("text", "value", "name", "title", "label"):
                if k in v:
                    return norm(v[k])
            try:
                return json.dumps(v, ensure_ascii=False)
            except Exception:
                return str(v)
        return str(v).strip()

    # Collect and dedupe people by name (keep longest title)
    by_name = {}
    for m in items:
        if not isinstance(m, dict):
            continue

        name = norm(m.get("name") or m.get("fullName") or m.get("person") or m.get("label"))
        title = norm(m.get("title") or m.get("job_title") or m.get("position") or m.get("role") or m.get("job"))

        # fallback: try to parse from a combined text line like "Name — Title"
        if not name and (m.get("text") or m.get("raw")):
            txt = norm(m.get("text") or m.get("raw"))
            mt = re.search(r"^\s*([A-Z][A-Za-z\.'\- ]{1,60})\s+[—\-–]\s+(.+)$", txt)
            if mt:
                name, title = mt.group(1).strip(), mt.group(2).strip()

        if not name:
            continue

        # keep the richer title if we see the same person again
        prev = by_name.get(name, "")
        if len(title) >= len(prev):
            by_name[name] = title

    people = [(n, t) for n, t in by_name.items()]
    people.sort(key=lambda nt: nt[0].lower())

    print("[Summary] Staff extracted:")
    for n, t in people:
        print(f" - {n}" + (f" — {t}" if t else ""))

    print(f"\n[Summary] Total people: {len(people)}")
    print(f"[Summary] Results saved to: {os.path.abspath(path)}")



def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run recon → crawl → staff scrape with one command.")
    parser.add_argument("url", help="Website home URL (e.g. https://www.crewsaders.com or crewsaders.com)")

    parser.add_argument("--skip-recon", action="store_true", help="Skip recon_actor.py")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip run_apify_from_recon.py")
    parser.add_argument("--skip-staff", action="store_true", help="Skip select_and_scrape_staff.py")

    parser.add_argument("--no-force-www", action="store_true", help="Do not force 'www.' on apex domains")
    parser.add_argument("--no-force-https", action="store_true", help="Do not force https scheme")

    parser.add_argument("--include-sitemaps", action="store_true",
                        help="Seed crawler with sitemap URLs (slower; default OFF).")

    parser.add_argument("--apify-token", dest="apify_token", help="APIFY_TOKEN value")
    parser.add_argument("--act-id", dest="apify_act_id", help="APIFY_ACT_ID override for Web Scraper")
    parser.add_argument("--proxy-group", dest="apify_proxy_group", help="APIFY_PROXY_GROUP to use")

    args = parser.parse_args()

    try:
        canonical_url = canonicalize_url(
            args.url,
            force_www=not args.no_force_www,
            force_https=not args.no_force_https,
        )
    except Exception as e:
        raise SystemExit(f"[Error] {e}")

    print(f"[Canonical URL] {canonical_url}")

    env = os.environ.copy()
    if args.apify_token:
        env["APIFY_TOKEN"] = args.apify_token
    if args.apify_act_id:
        env["APIFY_ACT_ID"] = args.apify_act_id
    if args.apify_proxy_group:
        env["APIFY_PROXY_GROUP"] = args.apify_proxy_group

    # 🔒 Force NO-SITEMAPS by default; allow opt-in via --include-sitemaps
    env["INCLUDE_SITEMAPS"] = "1" if args.include_sitemaps else "0"
    print(f"[Crawl settings] INCLUDE_SITEMAPS={env['INCLUDE_SITEMAPS']}")

    if not env.get("APIFY_TOKEN"):
        print("[Warn] APIFY_TOKEN is not set. Crawling actors will fail unless set via .env or --apify-token.")

    to_run: List[Tuple[str, str]] = []
    if not args.skip_recon:
        to_run.append(SCRIPTS[0])
    if not args.skip_crawl:
        to_run.append(SCRIPTS[1])
    if not args.skip_staff:
        to_run.append(SCRIPTS[2])

    t0 = time.monotonic()
    for step_name, script in to_run:
        run_script(step_name, script, canonical_url, env)

    total = time.monotonic() - t0
    summarize_results(STAFF_RESULTS_JSON)
    print(f"\n[Pipeline] Finished in {total:.1f}s")


if __name__ == "__main__":
    main()
