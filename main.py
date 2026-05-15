"""
Агрегатор цен на запчасти Ponsse с 3 сайтов.
"""

import asyncio
import os
import re
import time
from typing import Optional
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

app = FastAPI(title="Ponsse Parts Price Aggregator")

@app.get("/")
async def root():
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE: dict[str, tuple[float, list]] = {}
CACHE_TTL_SECONDS = 24 * 60 * 60

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

REQUEST_TIMEOUT = 15.0


def normalize_article(s: str) -> str:
    """Убирает пробелы, верхний регистр, кириллическая Р -> латинская P."""
    return re.sub(r"\s+", "", s).upper().replace("Р", "P")


def article_matches(article_query: str, title: str) -> bool:
    """
    P60055 найдёт: P60055, P60055CH, P60055ЛПС
    НЕ найдёт: P31635, P600551 (другой артикул)
    """
    q = normalize_article(article_query)
    t = normalize_article(title)
    idx = t.find(q)
    if idx == -1:
        return False
    # Символ ПЕРЕД вхождением не должен быть буквой/цифрой
    if idx > 0 and t[idx - 1].isalnum():
        return False
    return True


def parse_price(text: str) -> Optional[float]:
    """
    Извлекает первую цену из строки. Формат: '48 607,00 руб.' или '48607 руб.'
    Игнорирует числа > 50 000 000 (артефакты парсинга).
    """
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").strip()
    m = re.search(r"([\d][\d\s\.]*(?:,\d{1,2})?)\s*руб", cleaned)
    if not m:
        return None
    raw = m.group(1).replace(" ", "").replace("\xa0", "")
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    else:
        raw = raw.replace(".", "")
    try:
        val = float(raw)
        return val if val <= 50_000_000 else None
    except ValueError:
        return None


# ============================================================
# СКРАПЕР 1: saleponsse.ru
# ============================================================
async def scrape_saleponsse(client: httpx.AsyncClient, article: str) -> list[dict]:
    url = f"http://saleponsse.ru/advanced_search_result.php?keywords={quote_plus(article)}"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "saleponsse.ru", "error": f"Ошибка запроса: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")
    results = []
    seen = set()

    for a in soup.select('a[href*="product_info.php"]'):
        href = a.get("href", "")
        if href in seen:
            continue
        title = a.get_text(strip=True)
        if not title or len(title) < 3:
            continue
        # Только те товары, где искомый артикул реально есть в названии
        if not article_matches(article, title):
            continue
        seen.add(href)

        parent = a
        price = None
        for _ in range(6):
            parent = parent.parent
            if parent is None:
                break
            txt = parent.get_text(" ", strip=True)
            if "руб" in txt.lower():
                price = parse_price(txt)
                if price:
                    break

        full_url = href if href.startswith("http") else f"http://saleponsse.ru/{href.lstrip('/')}"
        results.append({"source": "saleponsse.ru", "title": title, "price": price, "currency": "RUB", "url": full_url})

    return results[:10] if results else [{"source": "saleponsse.ru", "error": "Ничего не найдено"}]


# ============================================================
# СКРАПЕР 2: store.lpskomi.ru
# Проблема с ценой: парсер захватывал много текста и склеивал числа.
# Решение: ищем самый маленький блок с "руб" (не длиннее 40 символов).
# ============================================================
async def scrape_lpskomi(client: httpx.AsyncClient, article: str) -> list[dict]:
    url = f"https://store.lpskomi.ru/search/?q={quote_plus(article)}&s=%D0%9D%D0%B0%D0%B9%D1%82%D0%B8"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "store.lpskomi.ru", "error": f"Ошибка запроса: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")
    results = []
    seen = set()

    for a in soup.select('a[href*="/catalog/"]'):
        href = a.get("href", "")
        if not re.search(r"/catalog/\d+/?$", href):
            continue
        title = a.get_text(strip=True)
        if not title or len(title) < 3 or title in seen:
            continue
        if not article_matches(article, title):
            continue
        seen.add(title)

        # Ищем цену: берём ближайший маленький блок с "руб"
        price = None
        parent = a
        for _ in range(5):
            parent = parent.parent
            if parent is None:
                break
            # Перебираем все теги внутри родителя — берём самый компактный с ценой
            for tag in parent.find_all(True):
                tag_txt = tag.get_text(" ", strip=True)
                if "руб" in tag_txt.lower() and 2 < len(tag_txt) < 40:
                    p = parse_price(tag_txt)
                    if p:
                        price = p
                        break
            if price:
                break

        full_url = href if href.startswith("http") else f"https://store.lpskomi.ru{href}"
        results.append({"source": "store.lpskomi.ru", "title": title, "price": price, "currency": "RUB", "url": full_url})

    return results[:10] if results else [{"source": "store.lpskomi.ru", "error": "Ничего не найдено"}]


# ============================================================
# СКРАПЕР 3: shop.gsperm.pro
# Их поиск рендерится через JS — прямой GET пустой.
# Решение: ищем товары через Яндекс (site:shop.gsperm.pro АРТИКУЛ),
# затем заходим на каждую страницу товара напрямую.
# ============================================================
async def scrape_gsperm(client: httpx.AsyncClient, article: str) -> list[dict]:
    yandex_url = f"https://yandex.ru/search/?text=site%3Ashop.gsperm.pro+{quote_plus(article)}&lr=2"
    try:
        resp = await client.get(yandex_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "shop.gsperm.pro", "error": f"Ошибка поиска: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")

    product_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        clean = re.search(r"(https?://shop\.gsperm\.pro/products/[^\s\"&?]+)", href)
        if clean:
            u = clean.group(1)
            if u not in product_urls:
                product_urls.append(u)

    if not product_urls:
        return [{"source": "shop.gsperm.pro", "error": "Ничего не найдено"}]

    tasks = [_fetch_gsperm_product(client, url, article) for url in product_urls[:5]]
    items = await asyncio.gather(*tasks, return_exceptions=True)
    results = [i for i in items if isinstance(i, dict) and i]

    return results if results else [{"source": "shop.gsperm.pro", "error": "Ничего не найдено"}]


async def _fetch_gsperm_product(client: httpx.AsyncClient, url: str, article: str) -> dict:
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception:
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else url.split("/")[-1]

    if not article_matches(article, title):
        return {}

    price = None
    # Ищем по классам price/cost
    for selector in ["[class*='price']", "[class*='cost']", "[class*='Price']"]:
        el = soup.select_one(selector)
        if el:
            p = parse_price(el.get_text(" ", strip=True))
            if p:
                price = p
                break
    # Если не нашли — берём первое число с "руб" на странице
    if not price:
        page_text = soup.get_text(" ")
        price = parse_price(page_text)

    return {"source": "shop.gsperm.pro", "title": title, "price": price, "currency": "RUB", "url": url}


# ============================================================
# ЭНДПОИНТЫ
# ============================================================
@app.get("/api/search")
async def search(article: str = Query(..., min_length=2)):
    article_clean = article.strip()
    cache_key = article_clean.lower()

    if cache_key in CACHE:
        ts, data = CACHE[cache_key]
        if time.time() - ts < CACHE_TTL_SECONDS:
            return {"article": article_clean, "cached": True, "results": data}

    async with httpx.AsyncClient() as client:
        results_lists = await asyncio.gather(
            scrape_saleponsse(client, article_clean),
            scrape_lpskomi(client, article_clean),
            scrape_gsperm(client, article_clean),
            return_exceptions=True,
        )

    all_results = []
    for r in results_lists:
        if isinstance(r, Exception):
            all_results.append({"source": "unknown", "error": str(r)})
        else:
            all_results.extend(r)

    CACHE[cache_key] = (time.time(), all_results)
    return {"article": article_clean, "cached": False, "results": all_results}


@app.delete("/api/cache")
async def clear_cache():
    CACHE.clear()
    return {"status": "ok", "message": "Кэш очищен"}


@app.get("/api/health")
async def health():
    return {"status": "ok", "cached_articles": len(CACHE)}
