#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from playwright.sync_api import sync_playwright

AMAZON_DOMAIN = "https://www.amazon.fr"

TARGETS = [
    "iphone 17", "iphone 17 air", "iphone 17 pro", "iphone 17 pro max",
    "iphone 16", "iphone 16 plus", "iphone 16 pro", "iphone 16 pro max",
    "iphone 15", "iphone 15 plus", "iphone 15 pro", "iphone 15 pro max",
    "iphone 14", "iphone 14 plus", "iphone 14 pro", "iphone 14 pro max",
]


def build_listing_url(base_url: str, page_num: int) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)

    for k in ["qid", "xpid", "ds", "ref", "sr"]:
        query.pop(k, None)

    if page_num > 1:
        query["page"] = [str(page_num)]

    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query, doseq=True), ""))


def normalize_title(s: str) -> str:
    return (s or "").lower().replace("+", " plus ").replace("gb", "go")


def match_product(title: str) -> bool:
    t = normalize_title(title)
    return any(target in t for target in TARGETS)


def main(config_path: str) -> None:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    base_url = cfg["listing_pages"][0]
    url = build_listing_url(base_url, 1)

    print(f"[INFO] Opening listing: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = browser.new_context(
            locale="fr-FR",
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        )

        page = context.new_page()

        # 🔥 Anti-bot script
        page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['fr-FR','fr'] });
        """)

        page.goto(url)

        time.sleep(5)

        html = page.content()

        # 🔥 DEBUG
        if "Toutes nos excuses" in html:
            print("[BLOCKED] Amazon bloque le scraping ❌")
            return
        else:
            print("[OK] Page chargée correctement ✅")

        cards = page.locator('[data-component-type="s-search-result"]')

        count = cards.count()
        print(f"[INFO] Products found: {count}")

        found = 0

        for i in range(min(count, 10)):
            card = cards.nth(i)

            try:
                title = card.locator("h2").inner_text()
                href = card.locator("a[href*='/dp/']").first.get_attribute("href")

                if href:
                    url_product = AMAZON_DOMAIN + href

                if match_product(title):
                    print("-" * 60)
                    print("MATCH")
                    print(title)
                    print(url_product)

                    found += 1

            except:
                continue

        print(f"[DONE] Matches: {found}")

        browser.close()


if __name__ == "__main__":
    import sys
    config = sys.argv[1]
    main(config)
