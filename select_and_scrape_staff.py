#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
select_and_scrape_staff.py
- Select likely staff/team URLs from cache
- Run Apify Web Scraper with robust hooks + pageFunction
- Extract structured staff data (name, title, linkedin, email, image)
- Save to staff_scrape_results.json
"""

import os
import sys
import time
import json
import re

from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

import requests


HERE = os.path.dirname(os.path.abspath(__file__))


# --- Environment / Config ---
APIFY_TOKEN = ((os.getenv("APIFY_TOKEN") or "").strip().strip('"').strip("'"))
ACT_ID = os.getenv("APIFY_ACT_ID", "apify~web-scraper").strip()  # safe default
APIFY_PROXY_GROUP = os.getenv("APIFY_PROXY_GROUP", os.getenv("APIFY_PROXY_GROUPS", "")).strip()

RESULTS_JSON = os.path.join(HERE, "staff_scrape_results.json")
CACHE_URLS_ALL = os.path.join(HERE, "cache_urls_all.json")

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
                os.environ.setdefault(k, v)
    except Exception:
        pass


load_env_file()  # load as early as possible



def proxy_config() -> Dict[str, Any]:
    conf: Dict[str, Any] = {"useApifyProxy": True}
    if APIFY_PROXY_GROUP:
        conf["apifyProxyGroups"] = [APIFY_PROXY_GROUP]
    return conf


def load_cache_urls() -> Dict[str, List[str]]:
    if not os.path.exists(CACHE_URLS_ALL):
        return {"internal": [], "external": [], "social": []}
    with open(CACHE_URLS_ALL, "r", encoding="utf-8") as f:
        data = json.load(f)
    for k in ("internal", "external", "social"):
        data[k] = data.get(k) or []
    return data


def canonical_home(url: str) -> str:
    # keep scheme+host, drop path/query/fragment
    try:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(url)
        netloc = p.netloc
        scheme = p.scheme or "https"
        return urlunparse((scheme, netloc, "", "", "", ""))
    except Exception:
        return url


ANCHORS = [
    "#team", "#people", "#staff", "#leadership", "#our-team",
    "#management", "#crew", "#about", "#the-team-behind-your-team",
    "#meet-the-team", "#our-people"
]

TEAM_KEYWORDS = re.compile(r"(team|people|staff|leadership|our-people|our-team|management|meet-the-team)", re.I)
JUNK = re.compile(r"(privacy|cookie|terms|policy|sitemap|login|signup|register|account|cart|basket)", re.I)


def select_staff_urls(home, urls_all=None):
    """
    Robustly build likely staff URLs.
    Accepts home as str | dict | list and normalizes to a URL string.
    urls_all is optional and may have {'internal': [...]}.
    """

    def norm_url(x):
        # Try to extract a URL string from various shapes
        if isinstance(x, str):
            return x.strip()
        if isinstance(x, dict):
            for k in ("url", "home", "base", "start", "href"):
                v = x.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""
        if isinstance(x, list) and x:
            # pick first string-ish item
            for v in x:
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    # 1) Normalize the homepage URL
    home_url = norm_url(home)
    if not home_url and urls_all:
        # try fallback sources inside urls_all
        home_url = norm_url(urls_all.get("home")) or norm_url((urls_all.get("internal") or [None])[0])

    if not home_url:
        raise ValueError("select_staff_urls: cannot determine a homepage URL")

    # Use your canonicalizer if it exists
    if 'canonicalize_url' in globals():
        home_url = canonicalize_url(home_url)
    elif 'canonical_url' in globals():
        home_url = canonical_url(home_url)

    base = home_url.rstrip("/")

    # 2) Always include homepage + common anchors
    anchors = [
        "#team", "#people", "#staff", "#leadership", "#our-team", "#management",
        "#crew", "#about", "#meet-the-team", "#our-people", "#the-team-behind-your-team",
        "#who-we-are", "#company", "#board", "#directors", "#founders"
    ]
    candidates = [home_url] + [base + a for a in anchors]

    # 3) Optionally include any internal pages that look staff-ish
    if urls_all and isinstance(urls_all.get("internal"), list):
        keywords = (
            "team", "people", "staff", "leadership", "management", "about",
            "who-we-are", "company", "board", "directors", "founders", "our-people",
            "our-team", "meet"
        )
        for u in urls_all["internal"]:
            if not isinstance(u, str):
                continue
            low = u.lower()
            if any(k in low for k in keywords):
                candidates.append(u.strip())

    # 4) De-dupe and keep only same-host URLs
    host = urlparse(base).hostname
    seen, out = set(), []
    for u in candidates:
        if not u or not isinstance(u, str):
            continue
        try:
            if urlparse(u).hostname == host and u not in seen:
                out.append(u)
                seen.add(u)
        except Exception:
            continue

    return out


# --------------------- Hooks & Page Function (STRINGS) ---------------------

PRE_NAV_HOOK = r"""[
  async (c) => {
    const { request, page, log } = c;

    // Ensure we respect fragments for requests like /#team
    try {
      if (request.url.includes('#')) {
        const frag = request.url.split('#')[1];
        if (frag) await page.evaluate((id) => { if (id) location.hash = id; }, frag);
      }
    } catch (_) {}

    // Simple UA tweak for stubborn sites
    try {
      const ua = await page.browser().userAgent();
      await page.setUserAgent(ua.replace(/HeadlessChrome/i, "Chrome"));
    } catch (_) {}

    // Lower navigation risk for SPA-ish sites
    await page.setDefaultNavigationTimeout(180000);
    await page.setDefaultTimeout(180000);
  }
]"""

POST_NAV_HOOK = r"""[
  async (c) => {
    const { request, page, log } = c;

    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const gentleScroll = async () => {
      try {
        await page.evaluate(async () => {
          await new Promise((resolve) => {
            let y = 0;
            const step = () => {
              y += 400;
              window.scrollTo({ top: y, behavior: 'instant' });
              if (y < document.body.scrollHeight) setTimeout(step, 120);
              else resolve();
            };
            step();
          });
        });
      } catch (_) {}
    };

    // Reveal menus and accordions
    try {
      await page.evaluate(() => {
        const sleep = (ms) => new Promise(r => setTimeout(r, ms));
        const toggles = [
          'button[aria-label*="menu" i]',
          '[data-testid*="menu" i]',
          '.hamburger',
          '.menu-toggle',
          'button[aria-expanded="false"]',
          '.nav-toggle',
          '.accordion-button'
        ];
        (async () => {
          for (const sel of toggles) {
            const els = Array.from(document.querySelectorAll(sel));
            for (const el of els) { el.click(); await sleep(120); }
          }
        })();
      });
    } catch (_) {}

    // Scroll to anchor if any
    try {
      let target = (request?.userData && request.userData.anchor) || "";
      if (!target && request.url.includes("#")) {
        target = request.url.split("#")[1] || "";
      }
      if (target) {
        await page.evaluate((id) => {
          const el = document.getElementById(id) || document.querySelector(`[name="${id}"]`);
          if (el) el.scrollIntoView({ behavior: 'instant', block: 'center' });
        }, target);
        await sleep(400);
      }
    } catch (_) {}

    // Gentle scroll to trigger lazy content
    await gentleScroll();
    await sleep(500);

    // Attempt to wait for “team-ish” text to show
    try {
      await page.waitForFunction(() => {
        const t = (document.body && document.body.innerText || "").toLowerCase();
        return /team|people|leadership|staff|our team|meet the team/.test(t);
      }, { timeout: 8000 });
    } catch (_) {}
  }
]"""

# === Full pageFunction: robust, site-agnostic staff parser ===
STAFF_PAGE_FUNCTION = r"""
async function pageFunction(context) {
  const { jQuery: $, log } = context;
  const out = { url: location.href, members: [], debug: { notes: [], counts: {}, sampleText: "" } };

  try {
    const results = [];
    const clean = (t) => (t || "").replace(/\s+/g, " ").trim();

    // --- Hydration guard (~6s max)
    for (let i = 0; i < 20; i++) {
      const txt = (document.body && document.body.innerText) || "";
      if (txt && txt.length > 400) break;
      // eslint-disable-next-line no-await-in-loop
      await new Promise(r => setTimeout(r, 300));
    }

    // --- Brand + roles
    const BRAND = [clean(document.title || ""), clean(($('meta[property="og:site_name"]').attr('content') || ""))]
      .filter(Boolean).map(s => s.toLowerCase());

    // Exact phrases we want to capture as-is (case-insensitive)
    const ROLE_PHRASES = [
      "Managing Director",
      "Operations Director",
      "Executive Director",
      "Regional Operations Manager",
      "Carpentry & Operations Manager",
      "Carpentry Manager",
      "National Crew Manager",
      "Head of Partnerships",
      "Bookings/Operations Officer",
      "Site Manager",
      "Support Ops"
    ];

    // Broad dictionary used for validation (not for truncation)
    const ROLE_WORDS = new RegExp(
      [
        "managing director","operations director","executive director",
        "regional operations manager","carpentry & operations manager","carpentry manager",
        "national crew manager","head of partnerships?","site manager(?:\\s*&\\s*support ops)?",
        "project manager","bookings\\/operations officer","bookings officer",
        "operations officer","ops officer","operations manager","general manager",
        "manager","supervisor","foreman","team lead","lead","head","principal","specialist",
        "consultant","engineer","technician","operative","crew","assistant","coordinator",
        "officer","executive","administrator","associate","analyst","designer","developer",
        "architect","ceo","cto","coo","cfo","chair","chairman","president","vp","vice president",
        "producer","editor","marketer","recruiter","hr","human resources","operations","logistics",
        "warehouse","md","ops director","operations director"
      ].join("|"),
      "i"
    );

    const ROLE_TOKENS = new Set([
      "managing","director","operations","executive","regional","carpentry","manager","national",
      "crew","head","partnerships","site","support","ops","bookings","officer","president","vice",
      "vp","ceo","cto","coo","cfo"
    ]);

    const normalizeRole = (r) => {
      let s = clean(r);
      s = s.replace(/patnerships/gi, "Partnerships");
      s = s.replace(/\bsite manager\b/gi, "Site Manager");
      s = s.replace(/\b(&\s*)?support\s*ops\b/gi, (m, amp) => (amp ? "& " : "") + "Support Ops");
      s = s.replace(/\s*&\s*/g, " & ");
      s = s.replace(/[.,;:]+$/, ""); // drop trailing punctuation
      return s;
    };

    const isBrandish = (n) => {
      const low = (n || "").toLowerCase();
      if (!low) return true;
      if (BRAND.includes(low)) return true;
      if (/crewsaders/i.test(low)) return true;
      return false;
    };

    const nameTokensOk = (parts) => {
      if (parts.length < 2 || parts.length > 3) return false;
      if (ROLE_TOKENS.has((parts[0] || "").toLowerCase())) return false;
      if (ROLE_TOKENS.has((parts[parts.length - 1] || "").toLowerCase())) return false;
      for (const p of parts) if (ROLE_TOKENS.has((p || "").toLowerCase())) return false;
      return true;
    };

    const looksLikeHumanName = (n) => {
      const s = clean(n);
      if (!s || s.length < 3 || s.length > 80) return false;
      if (isBrandish(s)) return false;
      const parts = s.split(/\s+/);
      let caps = 0;
      for (const p of parts) {
        // Title-Case tokens; allow Mc/Mac/O'
        if (/^(?:[A-Z][a-z'’\-]+|Mc[A-Z][a-z'’\-]+|Mac[A-Z][a-z'’\-]+|O'[A-Z][a-z'’\-]+)$/.test(p)) caps++;
      }
      if (caps < 2) return false;
      return nameTokensOk(parts);
    };

    const looksLikeRole = (t) => !!clean(t) && ROLE_WORDS.test(clean(t));

    const pushCandidate = (obj, src) => {
      const name     = clean(obj.name || "");
      const titleRaw = clean(obj.title || obj.role || obj.jobTitle || obj.position || "");
      const title    = titleRaw ? normalizeRole(titleRaw) : "";
      if (!looksLikeHumanName(name)) return;
      if (title && !looksLikeRole(title)) return; // allow empty; dedupe may pick better later
      results.push({ name, title, image: obj.image || "", linkedin: obj.linkedin || "", email: obj.email || "", _source: src });
    };

    // --- Team container
    let teamRootEl =
      document.getElementById("the-team-behind-your-team") ||
      Array.from(document.querySelectorAll("h1,h2,h3,h4")).find(h => /team|people|leadership|management|expert team|behind the scenes/i.test((h.innerText||"")));
    if (teamRootEl) {
      let cur = teamRootEl;
      while (cur && cur !== document.body && ((cur.innerText || "").trim().length < 200)) cur = cur.parentElement;
      teamRootEl = cur || teamRootEl;
    }
    let $teamRoot = teamRootEl ? $(teamRootEl) : null;
    if (!$teamRoot || !$teamRoot.length) {
      const TEAM_HINT = /(the\s+team\s+behind\s+your\s+team|our\s+team|meet\s+the\s+team|our\s+people|leadership|management|expert team|behind the scenes)/i;
      const cands = $("section, article, main, div").filter((_, el) => TEAM_HINT.test((el.innerText || "").trim()));
      if (cands.length) {
        let best = null, bestLen = 0;
        cands.each((_, el) => {
          const len = (el.innerText || "").trim().length;
          if (len > bestLen && len < 20000) { best = el; bestLen = len; }
        });
        $teamRoot = best ? $(best) : $("body");
      } else {
        $teamRoot = $("body");
      }
    }

    // --- Visible text to parse
    let teamText = clean(($teamRoot[0] && $teamRoot[0].innerText) || document.body.innerText || "");
    // Demote screaming ALL-CAPS hero words
    teamText = teamText.replace(/\b(THE|AND|OUR|YOUR|BEHIND|SCENES)\b/g, w => w.toLowerCase());
    out.debug.sampleText = teamText.slice(0, 1200);

    // Only de-glue Role→Name (avoid breaking McConnachie)
    teamText = teamText
      .replace(/\b(Director|Manager|Supervisor|Leader|Lead|Head|Partner|Owner|Founder|Producer|Engineer|Technician)([A-Z][a-z'’\-]+)/g, "$1 $2")
      .replace(/\s{2,}/g, " ")
      .trim();

    // --- Triplets: Name @handle Role
    const TOKEN = "(?:[A-Z][a-z'’\\-]+|Mc[A-Z][a-z'’\\-]+|Mac[A-Z][a-z'’\\-]+|O'[A-Z][a-z'’\\-]+)";
    const NAME_CORE = `${TOKEN}(?:\\s+${TOKEN}){1,2}`;
    const NAME_AT_RE = new RegExp(`(${NAME_CORE})\\s+@(\\w+)`, "g");

    function stripRolePrefix(n) {
      const parts = (n || "").split(/\s+/);
      let i = 0;
      while (i < parts.length && i < 2 && (ROLE_TOKENS.has(parts[i].toLowerCase()) || /^(of|the)$/i.test(parts[i]))) i++;
      return parts.slice(i).join(" ");
    }

    const atHits = [];
    let m;
    while ((m = NAME_AT_RE.exec(teamText)) !== null) {
      let raw = clean(m[1]).replace(/[.,:;]+$/, "");
      const name = stripRolePrefix(raw);
      if (!looksLikeHumanName(name)) continue;
      atHits.push({ name, start: m.index, end: NAME_AT_RE.lastIndex });
    }

    function roleFromSegment(seg) {
      let s = seg
        .replace(/@\w+/g, " ")
        .replace(/\b[a-z0-9_]{10,}\b/g, " ")
        .replace(/[|•·]+/g, " ")
        .replace(/\s{2,}/g, " ")
        .trim();
      if (!s) return "";

      const sLow = s.toLowerCase();

      const found = [];
      for (const phrase of ROLE_PHRASES) {
        const idx = sLow.indexOf(phrase.toLowerCase());
        if (idx !== -1) found.push({ idx, phrase });
      }
      if (found.length) {
        found.sort((a,b) => a.idx - b.idx);
        const first = found[0].phrase;
        if (/site manager/i.test(first) && /support ops/i.test(s)) {
          return normalizeRole("Site Manager & Support Ops");
        }
        return normalizeRole(first);
      }

      const head = s.match(/head of\s+([A-Za-z/&\s]{2,60})/i);
      if (head) return normalizeRole(`Head of ${head[1]}`);

      const combos = [
        /\b(Regional\s+Operations)\s+(Manager)\b/i,
        /\b(Carpentry\s*&\s*Operations)\s+(Manager)\b/i,
        /\b(Carpentry)\s+(Manager)\b/i,
        /\b(National\s+Crew)\s+(Manager)\b/i,
        /\b(Bookings\/Operations)\s+(Officer)\b/i,
        /\b(Operations)\s+(Director)\b/i,
        /\b(Executive)\s+(Director)\b/i,
        /\b(Site)\s+(Manager)\b/i
      ];
      for (const rx of combos) {
        const mm = s.match(rx);
        if (mm) return normalizeRole(`${mm[1]} ${mm[2]}`);
      }

      const tokens = s.split(/\s+/);
      const CORE = /^(Director|Manager|Officer|Lead|Head|Executive)$/i;
      for (let i = 0; i < tokens.length; i++) {
        if (CORE.test(tokens[i])) {
          const left  = tokens[i-1] && /^[A-Za-z/&]+$/.test(tokens[i-1]) ? tokens[i-1] : "";
          const left2 = tokens[i-2] && /^[A-Za-z/&]+$/.test(tokens[i-2]) ? tokens[i-2] : "";
          const leftSide = (left2 ? left2 + " " : "") + (left ? left + " " : "");
          return normalizeRole(`${leftSide}${tokens[i]}`);
        }
      }

      const generic = s.match(ROLE_WORDS);
      return generic ? normalizeRole(generic[0]) : "";
    }

    const LAST_RESORT_ROLE = /\b((Managing|Operations|Executive)\s+Director|(Regional\s+Operations|Carpentry\s*&\s*Operations|National\s+Crew)\s+Manager|Carpentry\s+Manager|Bookings\/Operations\s+Officer|Site\s+Manager(?:\s*&\s*Support\s+Ops)?)\b/i;

    function roleScore(r) {
      if (!r) return 0;
      const s = r.toLowerCase();
      let score = r.length;
      if (/(director|manager|officer|head)\b/.test(s)) score += 50;
      if (/(managing director|operations director|head of|regional operations|carpentry & operations|national crew|bookings\/operations)/.test(s)) score += 100;
      return score;
    }
    function pickBetterRole(a, b) {
      return roleScore(b) > roleScore(a) ? b : a;
    }

    let triplets = 0;
    for (let i = 0; i < atHits.length; i++) {
      const cur  = atHits[i];
      const next = atHits[i + 1];

      const forwardSeg  = teamText.slice(cur.end, next ? next.start : teamText.length);
      const backwardSeg = teamText.slice(Math.max(0, cur.start - 200), cur.start);

      let forwardRole  = roleFromSegment(forwardSeg);
      let backwardRole = roleFromSegment(backwardSeg);

      if (!forwardRole || forwardRole.length < 12) {
        const win = forwardSeg.slice(0, 120);
        const m2 = win.match(LAST_RESORT_ROLE);
        if (m2 && m2[0]) forwardRole = normalizeRole(m2[0]);
      }

      const role = pickBetterRole(forwardRole, backwardRole);

      results.push({ name: cur.name, title: role, _source: "triplet" });
      triplets++;
    }
    out.debug.counts.triplets = triplets;

    const inAny = (i) => atHits.some(h => i >= h.start && i < h.end);

    const NAME_RE = new RegExp(NAME_CORE, "g");
    const nameHits = [];
    let nm;
    while ((nm = NAME_RE.exec(teamText)) !== null) {
      if (inAny(nm.index)) continue;
      const nmText = clean(nm[0]).replace(/[.,:;]+$/, "");
      const parts = nmText.split(/\s+/);
      let idx = 0;
      while (idx < parts.length && idx < 2 && (ROLE_TOKENS.has(parts[idx].toLowerCase()) || /^(of|the)$/i.test(parts[idx]))) idx++;
      const nameOnly = parts.slice(idx).join(" ");
      if (!looksLikeHumanName(nameOnly)) continue;
      nameHits.push({ name: nameOnly, start: nm.index, end: NAME_RE.lastIndex });
    }
    nameHits.sort((a,b) => a.start - b.start);

    let seqPairs = 0;
    for (let i = 0; i < nameHits.length; i++) {
      const cur  = nameHits[i];
      const nextPlainStart = i + 1 < nameHits.length ? nameHits[i + 1].start : Infinity;
      const nextAtStart    = atHits.find(h => h.start > cur.end)?.start ?? Infinity;
      const segEnd         = Math.min(nextPlainStart, nextAtStart, teamText.length);
      const seg            = teamText.slice(cur.end, segEnd);
      const role           = roleFromSegment(seg);
      results.push({ name: cur.name, title: role, _source: "seq" });
      seqPairs++;
    }
    out.debug.counts.seqPairs = seqPairs;

    let microCount = 0;
    $('[itemscope][itemtype*="schema.org/Person"]').each((_, el) => {
      const $el = $(el);
      const name = clean($el.find('[itemprop="name"]').first().text()) || ($el.attr('itemprop') === 'name' ? clean($el.text()) : "");
      const job  = clean($el.find('[itemprop="jobTitle"],[itemprop="role"]').first().text());
      if (name) results.push({ name, title: job, _source: "microdata" });
      microCount++;
    });
    out.debug.counts.microdataPersons = microCount;

    let ldjsonBlocks = 0;
    function walkJSON(o) {
      if (!o || typeof o !== "object") return;
      if (Array.isArray(o)) { o.forEach(walkJSON); return; }
      const type = (o['@type'] || o['type'] || '').toString().toLowerCase();
      if (type.includes('organization') || type.includes('website') || type.includes('webpage')) {
      } else if (type.includes('person') || (o.name && (o.jobTitle || o.role || o.position))) {
        results.push({ name: o.name, title: o.jobTitle || o.role || o.position || o.title, _source: "ldjson" });
      }
      for (const k in o) walkJSON(o[k]);
    }
    $('script[type="application/ld+json"]').each((_, s) => {
      const txt = $(s).text() || "";
      try { const obj = JSON.parse(txt); walkJSON(obj); ldjsonBlocks++; } catch {}
    });
    out.debug.counts.ldjsonBlocks = ldjsonBlocks;

    let cardCount = 0;
    const CARD_SELECTOR = `
      [class*="team-card"], [class*="member-card"], [class*="profile-card"],
      [class*="person"], [class*="people"], [class*="crew"],
      [id*="team"] [class], [class*="team"],
      [id*="people"], [id*="staff"], [id*="leadership"], [class*="staff"], [class*="leadership"]
    `.replace(/\s+/g, " ");
    $((teamRootEl || document.body)).find(CARD_SELECTOR).each((_, el) => {
      const $el   = $(el);
      const name  = clean($el.find('h2, h3, .name, [class*="name"], [itemprop="name"]').first().text());
      const title = clean($el.find('.role, .title, [class*="title"], [class*="role"], [itemprop="jobTitle"]').first().text());
      if (name) results.push({ name, title, _source: "domCard" });
      cardCount++;
    });
    out.debug.counts.domCardsScanned = cardCount;

    const byName = new Map();
    for (const c of results) {
      const key = (c.name || "").toLowerCase();
      if (!key) continue;
      if (!byName.has(key)) { byName.set(key, c); continue; }
      const prev = byName.get(key);
      const prevScore = (prev.title ? prev.title.length : 0);
      const curScore  = (c.title ? c.title.length : 0);
      if (curScore > prevScore) byName.set(key, c);
    }
    out.members = Array.from(byName.values()).map(({ _source, ...rest }) => rest);

    function escapeRegex(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
    function salvageRoleForName(name, fullText) {
      if (!name) return "";
      const lower = fullText.toLowerCase();
      const pos = lower.indexOf(name.toLowerCase());
      let best = "";

      if (pos !== -1) {
        const fseg = fullText.slice(pos, Math.min(fullText.length, pos + 120));
        const f = roleFromSegment(fseg);
        if (f) best = f;

        const bseg = fullText.slice(Math.max(0, pos - 160), pos);
        const b = roleFromSegment(bseg);
        best = (best && roleScore(best) >= roleScore(b)) ? best : b;
      } else {
        const rx = new RegExp(escapeRegex(name) + "[\\s\\S]{0,120}", "i");
        const m = fullText.match(rx);
        if (m) {
          const f = roleFromSegment(m[0]);
          if (f) best = f;
        }
      }
      return best;
    }

    let salvageFilled = 0;
    for (const m of out.members) {
      if (!m.title) {
        const fixed = salvageRoleForName(m.name, teamText);
        if (fixed) { m.title = normalizeRole(fixed); salvageFilled++; }
      }
    }

    function roleAfterHandleForName(name, fullText) {
      if (!name) return "";
      const rx = new RegExp(
        (name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')) +
        "\\s+@\\w+\\s+" +
        "([A-Za-z][A-Za-z'’/&\\s]+?)" +
        "(?=" +
          "\\s+[A-Z][a-z'’\\-]+\\s+[A-Z][a-z'’\\-]+\\s+@|$" +
        ")"
      );
      const m = fullText.match(rx);
      return m ? m[1].trim() : "";
    }

    const WEAK_TITLE = /^(operations|crew)$/i;
    for (const m of out.members) {
      if (!m.title || WEAK_TITLE.test(m.title)) {
        const forced = roleAfterHandleForName(m.name, teamText);
        if (forced) m.title = normalizeRole(forced);
      }
    }

    out.debug.notes.push(`members_after_extraction=${out.members.length}`);
    if (salvageFilled) out.debug.notes.push(`salvage_filled=${salvageFilled}`);

  } catch (e) {
    out.debug.notes.push(`pageFunction error: ${e && e.message ? e.message : String(e)}`);
  }

  return out;
}
"""


# --------------------- Actor runner ---------------------

def start_staff_run(urls: List[str]) -> Dict[str, Any]:
    if not APIFY_TOKEN:
        raise RuntimeError("APIFY_TOKEN not set in environment (.env)")

    # Preserve fragments and also pass them via userData.anchor
    start_urls = []
    for u in urls:
        anchor = ""
        if "#" in u:
            _, frag = u.split("#", 1)
            anchor = frag.strip()
        start_urls.append({"url": u, "uniqueKey": u, "userData": {"anchor": anchor}})

    payload: Dict[str, Any] = {
        "startUrls": start_urls,
        "useChrome": True,
        "useStealth": True,
        "ignoreHttpsErrors": True,
        "injectJQuery": True,

        # Navigation tolerances (SPA-friendly)
        "waitUntil": ["networkidle2", "domcontentloaded"],
        "navigationTimeoutSecs": 180,
        "pageLoadTimeoutSecs": 180,

        "maxRequestsPerCrawl": max(5, len(start_urls)),
        "maxDepth": 0,
        "maxRequestRetries": 1,

        "proxyConfiguration": proxy_config(),

        "customData": {"readinessSelector": "body", "extraWaitMs": 600},

        # Hooks/page function as STRINGS (Apify expects strings)
        "preNavigationHooks": PRE_NAV_HOOK,
        "postNavigationHooks": POST_NAV_HOOK,
        "pageFunction": STAFF_PAGE_FUNCTION,

        # ✅ keep fragments like #team in requests
        "keepUrlFragments": True,
    }

    def try_start(actor_id: str) -> requests.Response:
        url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {APIFY_TOKEN}",
        }
        params = {"token": APIFY_TOKEN}  # also send as query param
        return requests.post(url, json=payload, headers=headers, params=params, timeout=180)

    actor = ACT_ID or "apify~web-scraper"
    r = try_start(actor)
    if r.status_code in (401, 403) and actor.lower() != "apify~web-scraper":
        print(f"[Apify] Falling back to 'apify~web-scraper' ({r.status_code})")
        r = try_start("apify~web-scraper")

    if r.status_code >= 400:
        print("[Apify ERROR]", r.status_code, r.text[:1000])
        r.raise_for_status()

    return r.json()["data"]


def wait_for_run(run_id: str, timeout_sec: int = 600, poll_secs: int = 3) -> Dict[str, Any]:
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


def save_results(items: List[Dict[str, Any]]) -> None:
    with open(RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] Saved {len(items)} items to {os.path.basename(RESULTS_JSON)}")


# --------------------- Main ---------------------

def main():
    import os, sys, json

    if len(sys.argv) < 2:
        print("Usage: python select_and_scrape_staff.py <url>")
        sys.exit(2)

    # 1) Canonicalize target
    raw = sys.argv[1].strip()
    canonical = canonical_url(raw) if 'canonical_url' in globals() else raw  # keep your existing helper
    if 'canonicalize_url' in globals():  # support older helper name
        canonical = canonicalize_url(raw)
    print(f">>\n")

    # 2) Load cache_urls_all.json if present (optional)
    HERE = os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(HERE, "cache_urls_all.json")

    urls_all = {"internal": [], "external": [], "social": []}
    loaded = False
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            for k in ("internal", "external", "social"):
                if isinstance(data.get(k), list):
                    urls_all[k] = data[k]
            loaded = True
        except Exception:
            loaded = False

    if loaded:
        print(f"[Info] Loaded cache_urls_all.json with counts: "
              f"{{'internal': {len(urls_all['internal'])}, 'external': {len(urls_all['external'])}, 'social': {len(urls_all['social'])}}}\n")
    else:
        print("[Info] cache_urls_all.json not found; proceeding with homepage + anchors only\n")

    # 3) Select likely staff URLs (uses your existing selector helper)
    if 'select_staff_urls' in globals():
        selected = select_staff_urls(canonical, urls_all)
    elif 'build_staff_candidates' in globals():
        selected = build_staff_candidates(canonical, urls_all)
    else:
        # very small fallback: homepage + common anchors
        anchors = ["#team", "#people", "#staff", "#leadership", "#our-team", "#management",
                   "#crew", "#about", "#meet-the-team", "#our-people", "#the-team-behind-your-team"]
        selected = [canonical] + [canonical.rstrip("/") + a for a in anchors]

    # de-dup & pretty print
    selected = list(dict.fromkeys([u for u in selected if isinstance(u, str) and u]))
    print("[Selected staff URLs]")
    print(json.dumps(selected, indent=2))
    print()

    # 4) Run Apify staff scrape (uses your existing helpers)
    print("[Staff scrape] Starting Apify run…")
    run = start_staff_run(selected)  # your function should return dict or ID
    run_id = run.get("id") if isinstance(run, dict) else run
    if run_id:
        print(f"[Staff scrape] Run id: {run_id}")

    final = wait_for_run(run_id, timeout_sec=720)  # your existing waiter prints live status/logs
    status = (final or {}).get("status", "UNKNOWN")
    print(f"\n[Staff scrape] Status: {status}\n")

    ds_id = (final or {}).get("defaultDatasetId")
    if status == "SUCCEEDED" and ds_id:
        items = fetch_dataset_items(ds_id)  # your existing fetcher
        out_path = os.path.join(HERE, "staff_scrape_results.json")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            print(f"[OK] Saved {len(items)} items to {out_path}")
        except Exception as e:
            print(f"[WARN] Could not save results: {e}")
    else:
        print("[Staff scrape] No dataset returned or run did not succeed.")
        if final and 'id' in final:
            rid = final['id']
            print(f"[Actor logs URL] https://api.apify.com/v2/actor-runs/{rid}/log?token={os.getenv('APIFY_TOKEN','')}&stream=true")



if __name__ == "__main__":
    main()
