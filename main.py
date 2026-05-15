"""
Агрегатор цен на запчасти Ponsse с 3 сайтов.
Эндпоинт GET /api/search?article=XXX возвращает результаты по всем источникам.
"""

import asyncio
import re
import time
from typing import Optional
from urllib.parse import quote_plus

import os
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI(title="Ponsse Parts Price Aggregator")

# Отдаём index.html на корневом URL
@app.get("/")
async def root():
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----- ПРОСТОЙ КЭШ В ПАМЯТИ (TTL 24 часа) -----
CACHE: dict[str, tuple[float, list]] = {}
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 часа

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
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


def parse_price(text: str) -> Optional[float]:
    """Извлекает первое число (цена) из строки. Поддерживает формат '586.728,00 руб.'"""
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").strip()
    # ищем последовательность цифр с разделителями
    match = re.search(r"[\d][\d\s\.\u00a0]*(?:,\d{1,2})?", cleaned)
    if not match:
        return None
    raw = match.group(0).replace(" ", "").replace("\xa0", "")
    # российский формат: точки = тысячи, запятая = десятичные
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    else:
        raw = raw.replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


# ============================================================
# СКРАПЕР 1: saleponsse.ru (движок ShopOS)
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

    # На ShopOS товары обычно лежат в таблицах со ссылками product_info.php
    product_links = soup.select('a[href*="product_info.php"]')
    seen = set()
    for a in product_links:
        href = a.get("href", "")
        if href in seen:
            continue
        # пропускаем картинки/превью без названия
        title = a.get_text(strip=True)
        if not title or len(title) < 3:
            continue
        seen.add(href)

        # ищем цену рядом с этой ссылкой — поднимаемся к ближайшему родителю-блоку
        parent = a
        price = None
        for _ in range(6):
            parent = parent.parent
            if parent is None:
                break
            txt = parent.get_text(" ", strip=True)
            if "руб" in txt.lower():
                # выделяем фрагмент с ценой
                m = re.search(r"([\d][\d\s\.\u00a0]*(?:,\d{1,2})?)\s*руб", txt)
                if m:
                    price = parse_price(m.group(1))
                    break

        full_url = href if href.startswith("http") else f"http://saleponsse.ru/{href.lstrip('/')}"
        results.append({
            "source": "saleponsse.ru",
            "title": title,
            "price": price,
            "currency": "RUB",
            "url": full_url,
        })

    if not results:
        return [{"source": "saleponsse.ru", "error": "Ничего не найдено"}]
    return results[:10]


# ============================================================
# СКРАПЕР 2: store.lpskomi.ru (1С-Битрикс)
# ============================================================
async def scrape_lpskomi(client: httpx.AsyncClient, article: str) -> list[dict]:
    url = f"https://store.lpskomi.ru/search/?q={quote_plus(article)}&s=Найти"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "store.lpskomi.ru", "error": f"Ошибка запроса: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    # Битриксовый поиск выдаёт ссылки на /catalog/<ID>/
    catalog_links = soup.select('a[href*="/catalog/"]')
    seen = set()
    for a in catalog_links:
        href = a.get("href", "")
        # отсеиваем категории — у товаров путь /catalog/<число>/
        if not re.search(r"/catalog/\d+/?$", href):
            continue
        title = a.get_text(strip=True)
        if not title or len(title) < 3 or title in seen:
            continue
        seen.add(title)

        # ищем цену в окрестности
        parent = a
        price = None
        for _ in range(8):
            parent = parent.parent
            if parent is None:
                break
            txt = parent.get_text(" ", strip=True)
            if "руб" in txt.lower():
                m = re.search(r"([\d][\d\s\.\u00a0]*(?:,\d{1,2})?)\s*руб", txt)
                if m:
                    price = parse_price(m.group(1))
                    break

        full_url = href if href.startswith("http") else f"https://store.lpskomi.ru{href}"
        results.append({
            "source": "store.lpskomi.ru",
            "title": title,
            "price": price,
            "currency": "RUB",
            "url": full_url,
        })

    if not results:
        return [{"source": "store.lpskomi.ru", "error": "Ничего не найдено"}]
    return results[:10]


# ============================================================
# СКРАПЕР 3: shop.gsperm.pro (AdVantShop.NET)
# ============================================================
async def scrape_gsperm(client: httpx.AsyncClient, article: str) -> list[dict]:
    url = f"https://shop.gsperm.pro/search?searchText={quote_plus(article)}"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "shop.gsperm.pro", "error": f"Ошибка запроса: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    # AdVantShop: ссылки на товары вида /products/<slug>
    product_links = soup.select('a[href*="/products/"]')
    seen = set()
    for a in product_links:
        href = a.get("href", "")
        title = a.get_text(strip=True)
        if not title or len(title) < 3:
            continue
        if title in seen:
            continue
        seen.add(title)

        parent = a
        price = None
        for _ in range(8):
            parent = parent.parent
            if parent is None:
                break
            txt = parent.get_text(" ", strip=True)
            if "руб" in txt.lower():
                m = re.search(r"([\d][\d\s\.\u00a0]*(?:,\d{1,2})?)\s*руб", txt)
                if m:
                    price = parse_price(m.group(1))
                    break

        full_url = href if href.startswith("http") else f"https://shop.gsperm.pro{href}"
        results.append({
            "source": "shop.gsperm.pro",
            "title": title,
            "price": price,
            "currency": "RUB",
            "url": full_url,
        })

    if not results:
        return [{"source": "shop.gsperm.pro", "error": "Ничего не найдено"}]
    return results[:10]


# ============================================================
# ОСНОВНОЙ ЭНДПОИНТ
# ============================================================
@app.get("/api/search")
async def search(article: str = Query(..., min_length=2, description="Артикул запчасти")):
    article_clean = article.strip()
    cache_key = article_clean.lower()

    # проверка кэша
    if cache_key in CACHE:
        ts, data = CACHE[cache_key]
        if time.time() - ts < CACHE_TTL_SECONDS:
            return {"article": article_clean, "cached": True, "results": data}

    # параллельные запросы ко всем трём сайтам
    async with httpx.AsyncClient() as client:
        tasks = [
            scrape_saleponsse(client, article_clean),
            scrape_lpskomi(client, article_clean),
            scrape_gsperm(client, article_clean),
        ]
        results_lists = await asyncio.gather(*tasks, return_exceptions=True)

    all_results = []
    for r in results_lists:
        if isinstance(r, Exception):
            all_results.append({"source": "unknown", "error": str(r)})
        else:
            all_results.extend(r)

    # пишем в кэш
    CACHE[cache_key] = (time.time(), all_results)

    return {"article": article_clean, "cached": False, "results": all_results}


@app.delete("/api/cache")
async def clear_cache():
    CACHE.clear()
    return {"status": "ok", "message": "Кэш очищен"}


@app.get("/api/health")
async def health():
    return {"status": "ok", "cached_articles": len(CACHE)}
