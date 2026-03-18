#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amazon smartphone tracker - JSON targets version

Objectif :
- Lire les produits cibles directement depuis les fichiers JSON de config
- Parcourir les listings Amazon par marque
- Garder seulement les produits qui matchent la liste cible
- Scraper les infos produit / offres vendeurs
- Mettre à jour un Excel historique avec une colonne timestamp par run
"""

import os
import re
import json
import time
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# -------------------------
# Constantes
# -------------------------

HEADLESS = True
DEFAULT_TIMEOUT_MS = 30000
SLEEP_LISTING_SEC = 1.2
SLEEP_PRODUCT_SEC = 1.5
MAX_PRODUCTS_PER_RUN = 120

AMAZON_DOMAIN = "https://www.amazon.fr"


# -------------------------
# Utils
# -------------------------

def normalize_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def safe_str(x) -> Optional[str]:
    s = normalize_text(x)
    return s if s else None


def parse_price_eur(text: Optional[str]) -> Optional[float]:
    if not text:
        return None

    t = normalize_text(text)
    t = t.replace("EUR", "€")

    m = re.search(r"(\d[\d\s.]*)\s*,\s*(\d{2})\s*€", t)
    if m:
        euros = m.group(1).replace(" ", "").replace(".", "")
        cents = m.group(2)
        try:
            return float(f"{int(euros)}.{cents}")
        except Exception:
            pass

    m = re.search(r"(\d[\d\s.]*)\s*€", t)
    if m:
        euros = m.group(1).replace(" ", "").replace(".", "")
        try:
            return float(int(euros))
        except Exception:
            pass

    return None


def extract_asin_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/dp/([A-Z0-9]{10})", url)
    if m:
        return m.group(1)
    m = re.search(r"/gp/product/([A-Z0-9]{10})", url)
    if m:
        return m.group(1)
    return None


def canonical_product_url(url: Optional[str]) -> Optional[str]:
    asin = extract_asin_from_url(url)
    if asin:
        return f"{AMAZON_DOMAIN}/dp/{asin}"
    return url


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


def normalize_for_match(s: str) -> str:
    s = normalize_text(s).lower()

    replacements = {
        "+": " plus ",
        "gb": "go",
        "iphone": "iphone",
        "titane noir": "noir titane",
        "titanium": "titane",
        "iphone+": "iphone plus",
        "pro max": "pro max",
        "air": "air",
    }

    for old, new in replacements.items():
        s = s.replace(old, new)

    s = re.sub(r"[^a-z0-9àâçéèêëîïôûùüÿñæœ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_targets_from_config(targets_list: Optional[list]) -> pd.DataFrame:
    if not targets_list:
        return pd.DataFrame(columns=["idsmartphone", "label_cible"])

    rows = []
    for i, label in enumerate(targets_list, start=1):
        rows.append({
            "idsmartphone": f"T{i}",
            "label_cible": normalize_text(label)
        })

    return pd.DataFrame(rows)


def guess_target_id(title: str, targets_df: pd.DataFrame) -> Optional[str]:
    if targets_df.empty or not title:
        return None

    title_n = normalize_for_match(title)

    best_id = None
    best_label = None
    best_score = -1

    for _, row in targets_df.iterrows():
        label = normalize_for_match(row.get("label_cible", ""))
        if not label:
            continue

        tokens = [tok for tok in label.split() if tok not in {"noir"}]
        score = sum(1 for tok in tokens if tok in title_n)

        if score > best_score:
            best_score = score
            best_id = safe_str(row.get("idsmartphone"))
            best_label = safe_str(row.get("label_cible"))

    if best_label:
        tokens = [tok for tok in normalize_for_match(best_label).split() if tok not in {"noir"}]
        threshold = max(2, len(tokens) - 1)
        if best_score >= threshold:
            return best_id

    return None


def get_label_from_id(targets_df: pd.DataFrame, target_id: Optional[str]) -> Optional[str]:
    if not target_id or targets_df.empty:
        return None
    m = targets_df[targets_df["idsmartphone"] == target_id]
    if m.empty:
        return None
    return safe_str(m.iloc[0]["label_cible"])


# -------------------------
# Browser helpers
# -------------------------

def make_browser(playwright):
    browser = playwright.chromium.launch(headless=HEADLESS)
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
    page.set_default_timeout(DEFAULT_TIMEOUT_MS)
    return browser, context, page


def accept_cookies_if_present(page) -> None:
    selectors = [
        'input[name="accept"]',
        '#sp-cc-accept',
        'button:has-text("Accepter")',
        'button:has-text("J’accepte")',
        'button:has-text("Tout accepter")',
    ]
    for sel in selectors:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=3000)
                time.sleep(1)
                return
        except Exception:
            pass


# -------------------------
# Listing scraping
# -------------------------

def scrape_listing_page(page, url: str) -> List[Dict[str, Any]]:
    print(f"[LISTING] {url}")
    page.goto(url, wait_until="domcontentloaded")
    time.sleep(4)
    accept_cookies_if_present(page)

    try:
        page.mouse.wheel(0, 1500)
        time.sleep(1.5)
        page.mouse.wheel(0, 1500)
        time.sleep(1.5)
    except Exception:
        pass

    try:
        print(f"  title={page.title()}")
    except Exception:
        pass

    try:
        body_txt = normalize_text(page.locator("body").inner_text())[:800]
        print(f"  body_preview={body_txt}")
    except Exception:
        pass

    products = []

    candidate_selectors = [
        '[data-component-type="s-search-result"][data-asin]',
        'div.s-result-item[data-asin]',
        '[data-asin]:has(h2)',
    ]

    cards = None
    count = 0

    for sel in candidate_selectors:
        try:
            loc = page.locator(sel)
            c = loc.count()
            print(f"  selector={sel} -> {c}")
            if c > 0:
                cards = loc
                count = c
                break
        except Exception:
            pass

    if cards is None or count == 0:
        return []

    for i in range(count):
        card = cards.nth(i)

        asin = safe_str(card.get_attribute("data-asin"))
        if not asin or len(asin) != 10:
            continue

        title = None
        product_url = None
        listing_price = None
        rating_value = None
        review_count = None

        try:
            title = safe_str(card.locator("h2").first.inner_text())
        except Exception:
            pass

        try:
            href = card.locator("a[href*='/dp/']").first.get_attribute("href")
            if href:
                product_url = href if href.startswith("http") else AMAZON_DOMAIN + href
                product_url = canonical_product_url(product_url)
        except Exception:
            pass

        try:
            whole = safe_str(card.locator(".a-price .a-offscreen").first.inner_text())
            listing_price = parse_price_eur(whole)
        except Exception:
            pass

        try:
            rv = safe_str(card.locator('span[aria-label*="étoiles"]').first.get_attribute("aria-label"))
            if rv:
                m = re.search(r"([0-9]+,[0-9]+|[0-9]+)", rv)
                if m:
                    rating_value = float(m.group(1).replace(",", "."))
        except Exception:
            pass

        try:
            rc = safe_str(card.locator("span.a-size-base").nth(0).inner_text())
            if rc:
                m = re.search(r"(\d[\d\s\.]*)", rc)
                if m:
                    val = m.group(1).replace(" ", "").replace(".", "")
                    if val.isdigit():
                        review_count = int(val)
        except Exception:
            pass

        if title or product_url:
            products.append(
                {
                    "asin": asin,
                    "title": title,
                    "url_product": product_url,
                    "listing_price": listing_price,
                    "ratingValue_listing": rating_value,
                    "reviewCount_listing": review_count,
                }
            )

    return products


# -------------------------
# Product page scraping
# -------------------------

def extract_value_after_label(text: str, label: str) -> Optional[str]:
    if not text:
        return None

    labels = ["Expéditeur", "Vendeur", "État", "Retours", "Paiement", "Assistance", "Plans d'assurance"]
    pattern = rf"{re.escape(label)}\s+(.*?)(?=\s+(?:{'|'.join(map(re.escape, labels))})\b|$)"
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if m:
        return safe_str(m.group(1))
    return None


def extract_product_main_info(page) -> Dict[str, Any]:
    out = {
        "title": None,
        "price_main": None,
        "availability_label": None,
        "seller_name_main": None,
        "shipped_by_main": None,
        "condition_label_main": None,
        "ratingValue": None,
        "reviewCount": None,
        "style_observed": None,
        "storage_observed": None,
    }

    try:
        out["title"] = safe_str(page.locator("#productTitle").first.inner_text())
    except Exception:
        pass

    for sel in [
        ".a-price .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .a-offscreen",
        "#corePrice_feature_div .a-offscreen",
    ]:
        try:
            txt = safe_str(page.locator(sel).first.inner_text())
            price = parse_price_eur(txt)
            if price is not None:
                out["price_main"] = price
                break
        except Exception:
            pass

    for sel in [
        "#availability span",
        "#outOfStock span",
        "#availabilityInsideBuyBox_feature_div",
    ]:
        try:
            txt = safe_str(page.locator(sel).first.inner_text())
            if txt:
                out["availability_label"] = txt
                break
        except Exception:
            pass

    try:
        txt = safe_str(page.locator("#acrPopover").first.get_attribute("title"))
        if txt:
            m = re.search(r"([0-9]+,[0-9]+|[0-9]+)", txt)
            if m:
                out["ratingValue"] = float(m.group(1).replace(",", "."))
    except Exception:
        pass

    try:
        txt = safe_str(page.locator("#acrCustomerReviewText").first.inner_text())
        if txt:
            m = re.search(r"([\d\s\.]+)", txt)
            if m:
                val = m.group(1).replace(" ", "").replace(".", "")
                if val.isdigit():
                    out["reviewCount"] = int(val)
    except Exception:
        pass

    try:
        style_label = page.locator('span:has-text("Style:")').first
        if style_label.count() > 0:
            out["style_observed"] = safe_str(style_label.locator("xpath=..").inner_text())
    except Exception:
        pass

    try:
        size_label = page.locator('span:has-text("Taille:")').first
        if size_label.count() > 0:
            out["storage_observed"] = safe_str(size_label.locator("xpath=..").inner_text())
    except Exception:
        pass

    try:
        right_panel = page.locator("#tabular-buybox").first
        if right_panel.count() > 0:
            txt = normalize_text(right_panel.inner_text())
            out["seller_name_main"] = extract_value_after_label(txt, "Vendeur")
            shipped = extract_value_after_label(txt, "Expéditeur")
            out["shipped_by_main"] = shipped or out["seller_name_main"]
            out["condition_label_main"] = extract_value_after_label(txt, "État")
    except Exception:
        pass

    if not out["seller_name_main"] or not out["condition_label_main"]:
        try:
            txt = normalize_text(page.locator("#desktop_buybox").first.inner_text())
            if not out["seller_name_main"]:
                out["seller_name_main"] = extract_value_after_label(txt, "Vendeur")
            if not out["shipped_by_main"]:
                shipped = extract_value_after_label(txt, "Expéditeur")
                out["shipped_by_main"] = shipped or out["seller_name_main"]
            if not out["condition_label_main"]:
                out["condition_label_main"] = extract_value_after_label(txt, "État")
        except Exception:
            pass

    return out


def go_to_all_offers_if_possible(page) -> Optional[str]:
    candidate_selectors = [
        'a:has-text("Neuf et d’occasion")',
        'a:has-text("Autres vendeurs")',
        'a:has-text("Voir toutes les options d’achat")',
        'a:has-text("Voir les offres")',
        '#buybox-see-all-buying-choices a',
        '#all-offers-display-scroller a',
    ]

    for sel in candidate_selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                href = loc.get_attribute("href")
                if href:
                    return href if href.startswith("http") else AMAZON_DOMAIN + href
        except Exception:
            pass

    return None


def scrape_offers_from_offers_page(page, offers_url: str, asin: str, product_title: Optional[str]) -> List[Dict[str, Any]]:
    print(f"    [OFFERS] {offers_url}")
    page.goto(offers_url, wait_until="domcontentloaded")
    time.sleep(2)

    offers = []

    candidate_blocks = [
        '[data-cy="aod-offer"]',
        '#aod-offer',
        '.aod-information-block',
    ]

    blocks = None
    for sel in candidate_blocks:
        loc = page.locator(sel)
        if loc.count() > 0:
            blocks = loc
            break

    if blocks is None or blocks.count() == 0:
        return offers

    for i in range(blocks.count()):
        b = blocks.nth(i)

        price = None
        seller_name = None
        shipped_by = None
        condition_label = None
        availability_label = None
        url_offer = offers_url

        try:
            txt = safe_str(b.locator(".a-price .a-offscreen").first.inner_text())
            price = parse_price_eur(txt)
        except Exception:
            pass

        try:
            txt = normalize_text(b.inner_text())
            availability_label = safe_str(txt[:300])
        except Exception:
            txt = ""

        seller_selectors = [
            'a[href*="seller="]',
            'a[href*="me="]',
            '.a-size-small.a-link-normal',
            '#aod-offer-soldBy a',
        ]
        for sel in seller_selectors:
            try:
                loc = b.locator(sel).first
                if loc.count() > 0:
                    seller_name = safe_str(loc.inner_text())
                    if seller_name:
                        break
            except Exception:
                pass

        if not seller_name:
            seller_name = extract_value_after_label(txt, "Vendeur")

        shipped_by = extract_value_after_label(txt, "Expéditeur") or seller_name

        m = re.search(r"\b(Neuf|Occasion|Reconditionné(?:\s*-\s*[A-Za-zÀ-ÿ]+)?)\b", txt, flags=re.IGNORECASE)
        if m:
            condition_label = safe_str(m.group(1))

        offers.append(
            {
                "asin": asin,
                "title": product_title,
                "seller_name": seller_name,
                "shipped_by": shipped_by,
                "condition_label": condition_label,
                "availability_label": availability_label,
                "price_eur": price,
                "url_offer": url_offer,
            }
        )

    return offers


def scrape_product_and_offers(page, url_product: str, asin: str) -> List[Dict[str, Any]]:
    print(f"  [PRODUCT] {url_product}")
    page.goto(url_product, wait_until="domcontentloaded")
    time.sleep(2)

    info = extract_product_main_info(page)

    rows = []

    rows.append(
        {
            "asin": asin,
            "title": info.get("title"),
            "seller_name": info.get("seller_name_main"),
            "shipped_by": info.get("shipped_by_main"),
            "condition_label": info.get("condition_label_main"),
            "availability_label": info.get("availability_label"),
            "price_eur": info.get("price_main"),
            "url_product": url_product,
            "url_offer": None,
            "ratingValue": info.get("ratingValue"),
            "reviewCount": info.get("reviewCount"),
            "style_observed": info.get("style_observed"),
            "storage_observed": info.get("storage_observed"),
        }
    )

    offers_url = go_to_all_offers_if_possible(page)
    if offers_url:
        extra_offers = scrape_offers_from_offers_page(page, offers_url, asin=asin, product_title=info.get("title"))
        for row in extra_offers:
            row["url_product"] = url_product
            row["ratingValue"] = info.get("ratingValue")
            row["reviewCount"] = info.get("reviewCount")
            row["style_observed"] = info.get("style_observed")
            row["storage_observed"] = info.get("storage_observed")
        rows.extend(extra_offers)

    return rows


# -------------------------
# Excel update
# -------------------------

def update_excel_history(df_run: pd.DataFrame, excel_file: str, sheet_name: str = "Suivi") -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    df_run = df_run.copy()
    df_run[timestamp] = df_run["price_eur"]

    key_cols = [
        "idsmartphone",
        "asin",
        "seller_name",
        "shipped_by",
        "condition_label",
    ]

    for c in key_cols:
        if c not in df_run.columns:
            df_run[c] = None

    df_run["__rowkey__"] = (
        df_run["idsmartphone"].fillna("").astype(str) + "||" +
        df_run["asin"].fillna("").astype(str) + "||" +
        df_run["seller_name"].fillna("").astype(str) + "||" +
        df_run["shipped_by"].fillna("").astype(str) + "||" +
        df_run["condition_label"].fillna("").astype(str)
    )

    fixed_cols = [
        "idsmartphone",
        "label_cible",
        "asin",
        "title",
        "seller_name",
        "shipped_by",
        "condition_label",
        "availability_label",
        "ratingValue",
        "reviewCount",
        "style_observed",
        "storage_observed",
        "url_product",
        "url_offer",
    ]

    keep_cols = ["__rowkey__"] + [c for c in fixed_cols if c in df_run.columns] + [timestamp]
    df_run = df_run[keep_cols].set_index("__rowkey__")

    if os.path.exists(excel_file):
        try:
            df_hist = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl", dtype=str)
            if not df_hist.empty and "__rowkey__" in df_hist.columns:
                df_hist = df_hist.set_index("__rowkey__")
            else:
                df_hist = pd.DataFrame().set_index(pd.Index([], name="__rowkey__"))
        except Exception as e:
            print(f"Lecture Excel impossible ({e}). Recréation.")
            df_hist = pd.DataFrame().set_index(pd.Index([], name="__rowkey__"))
    else:
        df_hist = pd.DataFrame().set_index(pd.Index([], name="__rowkey__"))

    df_merged = df_hist.combine_first(df_run)
    df_merged[timestamp] = df_run[timestamp].reindex(df_merged.index)

    for col in fixed_cols:
        if col in df_run.columns:
            if col in df_merged.columns:
                df_merged[col] = df_run[col].combine_first(df_merged[col])
            else:
                df_merged[col] = df_run[col]

    df_out = df_merged.reset_index()

    ts_cols = [c for c in df_out.columns if re.match(r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$", str(c))]
    stable_cols = ["__rowkey__"] + [c for c in fixed_cols if c in df_out.columns]
    other_cols = [c for c in df_out.columns if c not in stable_cols + ts_cols]

    df_out = df_out[stable_cols + sorted(ts_cols) + other_cols]

    os.makedirs(os.path.dirname(excel_file), exist_ok=True)
    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel mis à jour : {excel_file} | run={timestamp} | lignes={len(df_run)}")


# -------------------------
# Main
# -------------------------

def run_brand(config_path: str) -> None:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    brand = cfg.get("brand")
    output_excel = cfg["output_excel"]
    listing_pages = cfg.get("listing_pages", [])
    max_pages = int(cfg.get("max_pages", 5))
    targets_list = cfg.get("targets", [])

    targets_df = read_targets_from_config(targets_list)

    all_listing_products: List[Dict[str, Any]] = []
    seen_asins = set()

    with sync_playwright() as p:
        browser, context, page = make_browser(p)

        try:
            for base_url in listing_pages:
                for page_num in range(1, max_pages + 1):
                    url = build_listing_url(base_url, page_num)

                    try:
                        page_rows = scrape_listing_page(page, url)
                    except PlaywrightTimeoutError:
                        print(f"Timeout listing page {page_num} brand={brand}")
                        continue
                    except Exception as e:
                        print(f"Erreur listing page {page_num} brand={brand}: {e}")
                        continue

                    if not page_rows:
                        print(f"  -> page {page_num}: 0 produit")
                        continue

                    print(f"  -> page {page_num}: {len(page_rows)} produits")

                    for r in page_rows:
                        asin = r.get("asin")
                        title = r.get("title") or ""
                        matched_id = guess_target_id(title=title, targets_df=targets_df)

                        if not matched_id:
                            continue

                        r["idsmartphone"] = matched_id
                        r["label_cible"] = get_label_from_id(targets_df, matched_id)

                        if asin and asin not in seen_asins:
                            seen_asins.add(asin)
                            all_listing_products.append(r)

                    time.sleep(SLEEP_LISTING_SEC)

            all_listing_products = all_listing_products[:MAX_PRODUCTS_PER_RUN]

            final_rows = []

            for i, prod in enumerate(all_listing_products, start=1):
                asin = prod.get("asin")
                url_product = prod.get("url_product")
                listing_title = prod.get("title")

                if not asin or not url_product:
                    continue

                print(f"[{i}/{len(all_listing_products)}] {asin} - {listing_title}")

                try:
                    product_rows = scrape_product_and_offers(page, url_product, asin)
                except PlaywrightTimeoutError:
                    print(f"  !! timeout produit {asin}")
                    continue
                except Exception as e:
                    print(f"  !! erreur produit {asin}: {e}")
                    continue

                for row in product_rows:
                    row["idsmartphone"] = prod.get("idsmartphone")
                    row["label_cible"] = prod.get("label_cible")

                    if not row.get("title"):
                        row["title"] = listing_title
                    if row.get("ratingValue") is None:
                        row["ratingValue"] = prod.get("ratingValue_listing")
                    if row.get("reviewCount") is None:
                        row["reviewCount"] = prod.get("reviewCount_listing")
                    if row.get("price_eur") is None:
                        row["price_eur"] = prod.get("listing_price")

                    final_rows.append(row)

                time.sleep(SLEEP_PRODUCT_SEC)

        finally:
            context.close()
            browser.close()

    if not final_rows:
        print("Aucune donnée récupérée sur ce run.")
        return

    df_run = pd.DataFrame(final_rows)

    for col in [
        "idsmartphone", "label_cible", "asin", "title", "seller_name", "shipped_by",
        "condition_label", "availability_label", "url_product", "url_offer",
        "style_observed", "storage_observed"
    ]:
        if col in df_run.columns:
            df_run[col] = df_run[col].apply(safe_str)

    dedup_cols = ["idsmartphone", "asin", "seller_name", "shipped_by", "condition_label", "price_eur"]
    dedup_cols = [c for c in dedup_cols if c in df_run.columns]
    df_run = df_run.drop_duplicates(subset=dedup_cols).reset_index(drop=True)

    print(df_run.head(20).to_string(index=False))
    update_excel_history(df_run, excel_file=output_excel)


if __name__ == "__main__":
    import sys

    config = sys.argv[1] if len(sys.argv) > 1 else "amazon/configs/iphone.json"
    run_brand(config)
