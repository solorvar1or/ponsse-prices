"""
Агрегатор цен на запчасти Ponsse с 3 сайтов.
- saleponsse.ru   — парсинг HTML страницы поиска
- store.lpskomi.ru — парсинг публичного CSV прайса (надёжнее HTML)
- shop.gsperm.pro  — поиск через Google site: + парсинг страниц товаров
"""

import asyncio
import csv
import io
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
from contextlib import asynccontextmanager

# ============================================================
# CSV-прайс lpskomi: загружаем при старте, обновляем раз в 24 ч
# ============================================================
LPS_CSV_URL = "https://www1.lpskomi.ru/marketplase.csv"
LPS_STORE_BASE = "https://store.lpskomi.ru"

lps_cache: dict = {"rows": [], "loaded_at": 0.0}
LPS_TTL = 24 * 60 * 60


async def load_lps_csv(client: httpx.AsyncClient):
    """Скачивает CSV прайс lpskomi и кладёт в память."""
    try:
        resp = await client.get(LPS_CSV_URL, headers=HEADERS, timeout=20.0)
        resp.raise_for_status()
        # CSV в windows-1251 или utf-8-sig
        try:
            text = resp.content.decode("utf-8-sig")
        except Exception:
            text = resp.content.decode("cp1251", errors="replace")
        reader = csv.reader(io.StringIO(text), delimiter=";")
        rows = []
        for i, row in enumerate(reader):
            if i == 0:
                continue  # заголовок
            if len(row) < 5:
                continue
            # row: УИД; Наименование; Артикул; Артикул поставщика; Оптовая цена; ...
            rows.append({
                "title": row[1].strip(),
                "article": row[2].strip(),
                "price_str": row[4].strip(),
            })
        lps_cache["rows"] = rows
        lps_cache["loaded_at"] = time.time()
        print(f"[lpskomi CSV] загружено {len(rows)} позиций")
    except Exception as e:
        print(f"[lpskomi CSV] ошибка загрузки: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        await load_lps_csv(client)
    yield


app = FastAPI(title="Ponsse Parts Price Aggregator", lifespan=lifespan)


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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

REQUEST_TIMEOUT = 15.0


def normalize(s: str) -> str:
    """Убирает пробелы, верхний регистр, кириллическая Р -> латинская P."""
    return re.sub(r"\s+", "", s).upper().replace("Р", "P")


def article_matches(query: str, title: str) -> bool:
    """
    Артикул query должен присутствовать в title как отдельная часть.
    P60055 найдёт P60055, P60055CH, P60055ЛПС — но не P31635.
    """
    q = normalize(query)
    t = normalize(title)
    idx = t.find(q)
    if idx == -1:
        return False
    # символ ДО вхождения не должен быть буквой/цифрой
    if idx > 0 and t[idx - 1].isalnum():
        return False
    return True


def parse_price(text: str) -> Optional[float]:
    """Извлекает первую цену из строки (российский формат)."""
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").strip()
    m = re.search(r"([\d][\d\s\.]*(?:,\d{1,2})?)\s*руб", cleaned)
    if not m:
        # Для CSV — просто число
        m2 = re.match(r"^([\d]+(?:\.\d{1,2})?)$", cleaned.replace(",", "."))
        if m2:
            try:
                return float(m2.group(1))
            except ValueError:
                return None
        return None
    raw = m.group(1).replace(" ", "")
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
        results.append({"source": "saleponsse.ru", "title": title,
                        "price": price, "currency": "RUB", "url": full_url})

    return results[:10] if results else [{"source": "saleponsse.ru", "error": "Ничего не найдено"}]


# ============================================================
# СКРАПЕР 2: store.lpskomi.ru — поиск по CSV прайсу
# ============================================================
async def scrape_lpskomi(client: httpx.AsyncClient, article: str) -> list[dict]:
    # Обновляем CSV если устарел
    if time.time() - lps_cache["loaded_at"] > LPS_TTL or not lps_cache["rows"]:
        await load_lps_csv(client)

    rows = lps_cache["rows"]
    if not rows:
        return [{"source": "store.lpskomi.ru", "error": "Прайс не загружен"}]

    results = []
    for row in rows:
        title_full = f"{row['title']} {row['article']}"
        if not article_matches(article, title_full):
            continue

        price = None
        ps = row["price_str"].replace(",", ".")
        try:
            v = float(ps)
            price = v if 0 < v <= 50_000_000 else None
        except ValueError:
            pass

        # Ссылка на поиск — точного URL товара в CSV нет
        search_url = f"{LPS_STORE_BASE}/search/?q={quote_plus(row['article'])}"
        results.append({
            "source": "store.lpskomi.ru",
            "title": row["title"],
            "price": price,
            "currency": "RUB",
            "url": search_url,
        })

    return results[:10] if results else [{"source": "store.lpskomi.ru", "error": "Ничего не найдено"}]


# ============================================================
# СКРАПЕР 3: shop.gsperm.pro — поиск через Google
# ============================================================
async def scrape_gsperm(client: httpx.AsyncClient, article: str) -> list[dict]:
    # Ищем через Google: site:shop.gsperm.pro АРТИКУЛ
    google_url = (
        f"https://www.google.com/search?q=site%3Ashop.gsperm.pro+{quote_plus(article)}"
        f"&hl=ru&num=10"
    )
    google_headers = {**HEADERS, "Accept-Language": "ru-RU,ru;q=0.9"}
    try:
        resp = await client.get(google_url, headers=google_headers,
                                timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "shop.gsperm.pro", "error": f"Ошибка поиска: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")

    product_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Google оборачивает ссылки в /url?q=...
        clean = re.search(r"(https?://shop\.gsperm\.pro/products/[^\s\"&?]+)", href)
        if clean:
            u = clean.group(1).rstrip("/")
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
    for selector in ["[class*='price']", "[class*='cost']", "[class*='Price']"]:
        el = soup.select_one(selector)
        if el:
            p = parse_price(el.get_text(" ", strip=True))
            if p:
                price = p
                break
    if not price:
        price = parse_price(soup.get_text(" "))

    return {"source": "shop.gsperm.pro", "title": title,
            "price": price, "currency": "RUB", "url": url}


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
    return {"status": "ok", "cached_articles": len(CACHE), "lps_rows": len(lps_cache["rows"])}
