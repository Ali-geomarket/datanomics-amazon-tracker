#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from playwright.sync_api import sync_playwright

AMAZON_DOMAIN = "https://www.amazon.fr"

TARGETS = [
    "iphone 17",
    "iphone 17 air",
    "iphone 17 pro",
    "iphone 17 pro max",
    "iphone 16",
    "iphone 16 plus",
    "iphone 16 pro",
    "iphone 16 pro max",
    "iphone 15",
    "iphone 15 plus",
    "iphone 15 pro",
    "iphone 15 pro max",
    "iphone 14",
    "iphone 14 plus",
    "iphone 14 pro",
    "iphone 14 pro max",
]


def build_listing_url(base_url: str, page_num: int) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)

    for k in ["qid", "xpid", "ds", "ref", "sr", "dib", "dib_tag", "sbo", "__mk_fr_FR"]:
        query.pop(k, None)

    if page_num > 1:
        query["page"] = [str(page_num)]
    else:
        query.pop("page", None)

    new_query = urlencode(query, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def accept_cookies_if_present(page) -> None:
    selectors = [
        '#sp-cc-accept',
        'input[name="accept"]',
        'button:has-text("Accepter")',
        'button:has-text("Tout accepter")',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=3000)
                time.sleep(1)
                print(f"[OK] Cookies accepted with selector: {sel}")
                return
        except Exception:
            pass


def normalize_title(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("+", " plus ")
    s = s.replace("iphone+", "iphone plus")
    s = s.replace("gb", "go")
    s = " ".join(s.split())
    return s


def match_product(title: str) -> bool:
    if not title:
        return False

    t = normalize_title(title)

    for target in TARGETS:
        if target in t:
            return True

    return False


def main(config_path: str) -> None:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    base_url = cfg["listing_pages"][0]
    url = build_listing_url(base_url, 1)

    print(f"[INFO] Opening listing: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 2200},
        )
        page = context.new_page()
        page.set_default_timeout(30000)

        page.goto(url, wait_until="domcontentloaded")
        time.sleep(5)

        accept_cookies_if_present(page)

        try:
            page.mouse.wheel(0, 1200)
            time.sleep(2)
        except Exception:
            pass

        print(f"[INFO] Page title: {page.title()}")

        try:
            body_preview = page.locator("body").inner_text()[:1200]
            print("[INFO] Body preview:")
            print(body_preview)
        except Exception as e:
            print(f"[WARN] Impossible to read body: {e}")

        selectors = [
            '[data-component-type="s-search-result"][data-asin]',
            'div.s-result-item[data-asin]',
            '[data-asin]:has(h2)',
        ]

        chosen_selector = None
        cards = None

        for sel in selectors:
            try:
                loc = page.locator(sel)
                count = loc.count()
                print(f"[INFO] Selector {sel} -> {count}")
                if count > 0:
                    chosen_selector = sel
                    cards = loc
                    break
            except Exception as e:
                print(f"[WARN] Selector error {sel}: {e}")

        if not cards:
            print("[ERROR] No product cards found.")
            context.close()
            browser.close()
            return

        print(f"[OK] Selected selector: {chosen_selector}")

        found = 0
        matched = 0

        for i in range(cards.count()):
            if found >= 10:
                break

            card = cards.nth(i)

            try:
                asin = card.get_attribute("data-asin")
            except Exception:
                asin = None

            if not asin or len(asin) != 10:
                continue

            title = None
            url_product = None

            try:
                title = card.locator("h2").first.inner_text().strip()
            except Exception:
                pass

            try:
                href = card.locator("a[href*='/dp/']").first.get_attribute("href")
                if href:
                    url_product = href if href.startswith("http") else AMAZON_DOMAIN + href
            except Exception:
                pass

            found += 1

            if not match_product(title):
                continue

            matched += 1

            print("-" * 80)
            print("[MATCH]")
            print(f"ASIN  : {asin}")
            print(f"TITLE : {title}")
            print(f"URL   : {url_product}")

        print(f"[DONE] Number of cards inspected: {found}")
        print(f"[DONE] Number of matched products: {matched}")

        context.close()
        browser.close()


if __name__ == "__main__":
    import sys
    config = sys.argv[1] if len(sys.argv) > 1 else "amazon/configs/iphone.json"
    main(config)
