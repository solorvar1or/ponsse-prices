"""
Агрегатор цен на запчасти Ponsse с 5 источников:
- saleponsse.ru    — парсинг HTML
- store.lpskomi.ru — CSV прайс (скачивается автоматически)
- Прайс 2022       — Excel файл (загружается вручную)
- Прайс 2021       — Excel файл (загружается вручную)
- shop.gsperm.pro  — Google site: (резервный)
"""

import asyncio
import csv
import io
import os
import re
import time
from typing import Optional
from urllib.parse import quote_plus
from contextlib import asynccontextmanager

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

# ============================================================
# НОРМАЛИЗАЦИЯ АРТИКУЛОВ
# ============================================================
CYR_TO_LAT = {"Р": "P", "С": "C", "В": "B", "А": "A",
               "Е": "E", "О": "O", "Х": "X", "К": "K"}

def normalize_article(s: str) -> str:
    s = s.strip().upper()
    result = [CYR_TO_LAT.get(ch, ch) for ch in s]
    return re.sub(r"[\s\-]+", "", "".join(result))

def strip_zeros(s: str) -> str:
    return re.sub(r"^0+(\d)", r"\1", s)

def get_variants(query: str) -> set:
    base = normalize_article(query)
    variants = {base}
    stripped = strip_zeros(base)
    if stripped != base:
        variants.add(stripped)
    if base and base[0].isdigit() and not base.startswith("0"):
        variants.add("0" + base)
    return variants

def article_matches_csv(query: str, csv_article: str) -> bool:
    """Сравнивает артикул из запроса с артикулом из таблицы."""
    query_variants = get_variants(query)
    target = normalize_article(csv_article)
    target_stripped = strip_zeros(target)
    for qv in query_variants:
        qv_stripped = strip_zeros(qv)
        for q in {qv, qv_stripped}:
            if not q:
                continue
            for t in {target, target_stripped}:
                if not t:
                    continue
                if t == q:
                    return True
                if t.startswith(q):
                    rest = t[len(q):]
                    if rest and rest[0].isdigit():
                        continue
                    return True
    return False

def article_matches_title(query: str, title: str) -> bool:
    """Ищет артикул в названии товара."""
    tokens = re.findall(r"[A-ZА-ЯЁa-zа-яё0-9][A-ZА-ЯЁa-zа-яё0-9\-\.]*", title)
    for token in tokens:
        if article_matches_csv(query, token):
            return True
    return False

# ============================================================
# ХРАНИЛИЩЕ ДАННЫХ
# ============================================================
LPS_CSV_URL = "https://www1.lpskomi.ru/marketplase.csv"
LPS_STORE_BASE = "https://store.lpskomi.ru"

# Прайсы в памяти
lps_cache:   dict = {"rows": [], "loaded_at": 0.0}
price_2022:  dict = {"rows": [], "name": "Прайс 2022"}
price_2021:  dict = {"rows": [], "name": "Прайс 2021"}

LPS_TTL = 24 * 60 * 60

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


def parse_excel_pricelist(content: bytes, price_col_hint: str = None) -> list[dict]:
    """
    Читает Excel прайс с колонками Part Number + цена.
    Возвращает список {"article": ..., "price": ...}
    """
    try:
        import pandas as pd
        df = pd.read_excel(io.BytesIO(content), sheet_name=0, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]

        # Находим колонку с артикулом
        art_col = None
        for c in df.columns:
            if "part" in c.lower() or "артикул" in c.lower() or "article" in c.lower():
                art_col = c
                break
        if not art_col:
            art_col = df.columns[0]

        # Находим колонку с ценой
        price_col = None
        if price_col_hint and price_col_hint in df.columns:
            price_col = price_col_hint
        else:
            for c in df.columns:
                cl = c.lower()
                if "rub" in cl or "цена" in cl or "price" in cl or "руб" in cl:
                    price_col = c
                    break
        if not price_col:
            # берём последнюю числовую колонку
            for c in reversed(df.columns):
                if c != art_col:
                    price_col = c
                    break

        rows = []
        for _, row in df.iterrows():
            art = str(row[art_col]).strip() if pd.notna(row[art_col]) else ""
            if not art or art.lower() == "nan":
                continue
            price = None
            if price_col:
                try:
                    v = float(str(row[price_col]).replace(",", ".").replace(" ", ""))
                    if 0 < v <= 50_000_000:
                        price = v
                except (ValueError, TypeError):
                    pass
            rows.append({"article": art, "price": price})
        return rows
    except Exception as e:
        print(f"Ошибка чтения Excel: {e}")
        return []


async def load_lps_csv(client: httpx.AsyncClient):
    try:
        resp = await client.get(LPS_CSV_URL, headers=HEADERS, timeout=20.0)
        resp.raise_for_status()
        try:
            text = resp.content.decode("utf-8-sig")
        except Exception:
            text = resp.content.decode("cp1251", errors="replace")
        reader = csv.reader(io.StringIO(text), delimiter=";")
        rows = []
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if len(row) < 5:
                continue
            rows.append({
                "title": row[1].strip(),
                "article": row[2].strip(),
                "price_str": row[4].strip(),
            })
        lps_cache["rows"] = rows
        lps_cache["loaded_at"] = time.time()
        print(f"[lpskomi CSV] загружено {len(rows)} позиций")
    except Exception as e:
        print(f"[lpskomi CSV] ошибка: {e}")


# Пробуем загрузить Excel прайсы из папки (если загружены заранее)
def try_load_excel_from_disk():
    base = os.path.dirname(__file__)
    for fname, store, hint in [
        ("pricelist_2022.xlsx", price_2022, "New RUB2"),
        ("pricelist_2021.xlsx", price_2021, "RUB2"),
    ]:
        path = os.path.join(base, fname)
        if os.path.exists(path):
            with open(path, "rb") as f:
                rows = parse_excel_pricelist(f.read(), hint)
            store["rows"] = rows
            print(f"[{fname}] загружено {len(rows)} позиций")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try_load_excel_from_disk()
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


def parse_price_str(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").strip()
    m = re.search(r"([\d][\d\s\.]*(?:,\d{1,2})?)\s*руб", cleaned)
    if not m:
        try:
            return float(cleaned.replace(",", "."))
        except ValueError:
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
        if not article_matches_title(article, title):
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
                price = parse_price_str(txt)
                if price:
                    break

        full_url = href if href.startswith("http") else f"http://saleponsse.ru/{href.lstrip('/')}"
        results.append({"source": "saleponsse.ru", "title": title,
                        "price": price, "currency": "RUB", "url": full_url})

    return results[:10] if results else [{"source": "saleponsse.ru", "error": "Ничего не найдено"}]


# ============================================================
# СКРАПЕР 2: store.lpskomi.ru — CSV
# ============================================================
async def scrape_lpskomi(client: httpx.AsyncClient, article: str) -> list[dict]:
    if time.time() - lps_cache["loaded_at"] > LPS_TTL or not lps_cache["rows"]:
        await load_lps_csv(client)

    rows = lps_cache["rows"]
    if not rows:
        return [{"source": "store.lpskomi.ru", "error": "Прайс не загружен"}]

    results = []
    for row in rows:
        if not article_matches_csv(article, row["article"]) and \
           not article_matches_title(article, row["title"]):
            continue
        price = None
        try:
            v = float(row["price_str"].replace(",", "."))
            price = v if 0 < v <= 50_000_000 else None
        except ValueError:
            pass
        search_url = f"{LPS_STORE_BASE}/search/?q={quote_plus(row['article'])}"
        results.append({"source": "store.lpskomi.ru", "title": row["title"],
                        "price": price, "currency": "RUB", "url": search_url})

    return results[:10] if results else [{"source": "store.lpskomi.ru", "error": "Ничего не найдено"}]


# ============================================================
# ПОИСК ПО EXCEL ПРАЙСУ (общая функция)
# ============================================================
def search_excel_pricelist(store: dict, article: str, source_name: str, base_url: str) -> list[dict]:
    rows = store.get("rows", [])
    if not rows:
        return [{"source": source_name, "error": "Прайс не загружен — загрузите файл через /upload"}]

    results = []
    for row in rows:
        if not article_matches_csv(article, row["article"]):
            continue
        results.append({
            "source": source_name,
            "title": f"Арт. {row['article']}",
            "price": row["price"],
            "currency": "RUB",
            "url": base_url + quote_plus(row["article"]),
        })

    return results[:10] if results else [{"source": source_name, "error": "Ничего не найдено"}]


# ============================================================
# СКРАПЕР 5: shop.gsperm.pro (резервный, через Google)
# ============================================================
async def scrape_gsperm(client: httpx.AsyncClient, article: str) -> list[dict]:
    google_url = (
        f"https://www.google.com/search?q=site%3Ashop.gsperm.pro+{quote_plus(article)}&hl=ru&num=10"
    )
    try:
        resp = await client.get(google_url, headers=HEADERS,
                                timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "shop.gsperm.pro", "error": f"Ошибка поиска: {e}"}]

    soup = BeautifulSoup(resp.text, "lxml")
    product_urls = []
    for a in soup.find_all("a", href=True):
        clean = re.search(r"(https?://shop\.gsperm\.pro/products/[^\s\"&?]+)", a["href"])
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
    if not article_matches_title(article, title):
        return {}
    price = None
    for selector in ["[class*='price']", "[class*='cost']", "[class*='Price']"]:
        el = soup.select_one(selector)
        if el:
            p = parse_price_str(el.get_text(" ", strip=True))
            if p:
                price = p
                break
    if not price:
        price = parse_price_str(soup.get_text(" "))
    return {"source": "shop.gsperm.pro", "title": title,
            "price": price, "currency": "RUB", "url": url}


# ============================================================
# ЭНДПОИНТЫ
# ============================================================

@app.get("/api/search")
async def search(article: str = Query(..., min_length=2)):
    article_clean = article.strip()
    cache_key = normalize_article(article_clean)

    if cache_key in CACHE:
        ts, data = CACHE[cache_key]
        if time.time() - ts < CACHE_TTL_SECONDS:
            return {"article": article_clean, "cached": True, "results": data}

    loop = asyncio.get_event_loop()
    r2022 = await loop.run_in_executor(None, search_excel_pricelist,
        price_2022, article_clean, "Прайс 2022",
        "https://saleponsse.ru/advanced_search_result.php?keywords=")
    r2021 = await loop.run_in_executor(None, search_excel_pricelist,
        price_2021, article_clean, "Прайс 2021",
        "https://saleponsse.ru/advanced_search_result.php?keywords=")

    async with httpx.AsyncClient() as client:
        results_lists = await asyncio.gather(
            scrape_saleponsse(client, article_clean),
            scrape_lpskomi(client, article_clean),
            return_exceptions=True,
        )
    results_lists = list(results_lists) + [r2022, r2021]

    all_results = []
    for r in results_lists:
        if isinstance(r, Exception):
            all_results.append({"source": "unknown", "error": str(r)})
        elif isinstance(r, list):
            all_results.extend(r)

    CACHE[cache_key] = (time.time(), all_results)
    return {"article": article_clean, "cached": False, "results": all_results}


@app.post("/api/upload/{year}")
async def upload_pricelist(year: int, file: UploadFile = File(...)):
    """Загрузка Excel прайса. year = 2022 или 2021."""
    if year not in (2022, 2021):
        return JSONResponse({"error": "Поддерживается только 2022 или 2021"}, status_code=400)
    content = await file.read()
    hint = "New RUB2" if year == 2022 else "RUB2"
    rows = parse_excel_pricelist(content, hint)
    store = price_2022 if year == 2022 else price_2021
    store["rows"] = rows
    # Сохраняем на диск чтобы пережить перезапуск
    fname = f"pricelist_{year}.xlsx"
    path = os.path.join(os.path.dirname(__file__), fname)
    with open(path, "wb") as f:
        f.write(content)
    CACHE.clear()
    return {"status": "ok", "rows": len(rows), "year": year}


@app.get("/api/status")
async def status():
    return {
        "lps_rows": len(lps_cache["rows"]),
        "price_2022_rows": len(price_2022["rows"]),
        "price_2021_rows": len(price_2021["rows"]),
        "cached_articles": len(CACHE),
    }


@app.delete("/api/cache")
async def clear_cache():
    CACHE.clear()
    return {"status": "ok"}
