"""
Агрегатор цен на запчасти Ponsse.
Источники: saleponsse.ru, store.lpskomi.ru, Прайс 2022, Прайс 2021
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
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd

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
    query_variants = get_variants(query)
    target = normalize_article(str(csv_article))
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
    tokens = re.findall(r"[A-ZА-ЯЁa-zа-яё0-9][A-ZА-ЯЁa-zа-яё0-9\-\.]*", title)
    for token in tokens:
        if article_matches_csv(query, token):
            return True
    return False

# ============================================================
# ХРАНИЛИЩЕ
# ============================================================
LPS_CSV_URL = "https://www1.lpskomi.ru/marketplase.csv"
LPS_STORE_BASE = "https://store.lpskomi.ru"

lps_cache:  dict = {"rows": [], "loaded_at": 0.0}
price_2022: dict = {"rows": []}
price_2021: dict = {"rows": []}
price_ft:   dict = {"rows": []}  # Forest Technologies
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


def load_excel(path: str, price_col: str) -> list[dict]:
    """Читает Excel прайс, возвращает список {article, price}."""
    try:
        df = pd.read_excel(path, sheet_name=0, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]
        rows = []
        for _, row in df.iterrows():
            art = str(row.get("Part Number", "")).strip()
            if not art or art.lower() == "nan":
                continue
            price = None
            try:
                v = float(str(row.get(price_col, "")).replace(",", ".").replace(" ", ""))
                if 0 < v <= 50_000_000:
                    price = round(v, 2)
            except (ValueError, TypeError):
                pass
            rows.append({"article": art, "price": price})
        return rows
    except Exception as e:
        print(f"[Excel] Ошибка чтения {path}: {e}")
        return []


def parse_ft_articles(raw: str) -> list[str]:
    """
    Разбирает сложные артикулы Forest Technologies:
    'P51209 LT'                -> ['P51209LT', 'P51209']
    '6350588 LT / Р0061538 LT' -> ['6350588LT', '6350588', 'P0061538LT', 'P0061538']
    '0059109 = 7150100'        -> ['0059109', '7150100']
    """
    if not raw or str(raw).strip().lower() in ("nan", "артикул", ""):
        return []
    parts = re.split(r"[/=]", str(raw))
    result = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        norm = normalize_article(part)
        if not norm or not norm[0].isalnum():
            continue
        result.append(norm)
        # вариант без суффикса LT
        no_lt = re.sub(r"LT$", "", norm).strip()
        if no_lt != norm and no_lt:
            result.append(no_lt)
    return list(dict.fromkeys(result))


def load_ft_excel(path: str) -> list[dict]:
    """Читает прайс Forest Technologies со всех листов."""
    try:
        xl = pd.ExcelFile(path)
        rows = []
        for sheet in xl.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)
            for _, row in df.iterrows():
                art_raw = str(row.iloc[2]).strip() if len(row) > 2 else ""
                name    = str(row.iloc[3]).strip() if len(row) > 3 else ""
                price_raw = str(row.iloc[5]).strip() if len(row) > 5 else ""
                articles = parse_ft_articles(art_raw)
                if not articles:
                    continue
                price = None
                try:
                    v = float(price_raw.replace(" ", "").replace(",", "."))
                    if 0 < v <= 50_000_000:
                        price = round(v, 2)
                except (ValueError, TypeError):
                    pass
                for art in articles:
                    rows.append({"article": art, "title": name, "price": price})
        return rows
    except Exception as e:
        print(f"[FT Excel] Ошибка: {e}")
        return []


def get_base_dir() -> str:
    """Возвращает папку где лежит main.py."""
    return os.path.dirname(os.path.abspath(__file__))


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
            rows.append({"title": row[1].strip(), "article": row[2].strip(), "price_str": row[4].strip()})
        lps_cache["rows"] = rows
        lps_cache["loaded_at"] = time.time()
        print(f"[lpskomi] загружено {len(rows)} позиций")
    except Exception as e:
        print(f"[lpskomi] ошибка: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    base = get_base_dir()
    print(f"[startup] base dir: {base}")
    print(f"[startup] files: {os.listdir(base)}")

    # Загружаем Excel прайсы
    p22 = os.path.join(base, "pricelist_2022.xlsx")
    p21 = os.path.join(base, "pricelist_2021.xlsx")

    if os.path.exists(p22):
        rows = load_excel(p22, "New RUB2")
        price_2022["rows"] = rows
        print(f"[2022] загружено {len(rows)} позиций")
    else:
        print(f"[2022] файл не найден: {p22}")

    if os.path.exists(p21):
        rows = load_excel(p21, "RUB2")
        price_2021["rows"] = rows
        print(f"[2021] загружено {len(rows)} позиций")
    else:
        print(f"[2021] файл не найден: {p21}")

    pft = os.path.join(base, "pricelist_ft.xlsx")
    if os.path.exists(pft):
        rows = load_ft_excel(pft)
        price_ft["rows"] = rows
        print(f"[FT] загружено {len(rows)} позиций")
    else:
        print(f"[FT] файл не найден: {pft}")

    async with httpx.AsyncClient() as client:
        await load_lps_csv(client)
    yield


app = FastAPI(title="Ponsse Parts Price Aggregator", lifespan=lifespan)

@app.get("/")
async def root():
    return FileResponse(os.path.join(get_base_dir(), "index.html"))

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

CACHE: dict[str, tuple[float, list]] = {}
CACHE_TTL = 24 * 60 * 60


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
# СКРАПЕРЫ
# ============================================================
async def scrape_saleponsse(client: httpx.AsyncClient, article: str) -> list[dict]:
    url = f"http://saleponsse.ru/advanced_search_result.php?keywords={quote_plus(article)}"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        return [{"source": "saleponsse.ru", "error": f"Ошибка: {e}"}]

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


def search_excel(store: dict, article: str, source_name: str) -> list[dict]:
    rows = store.get("rows", [])
    if not rows:
        return [{"source": source_name, "error": "Файл не загружен"}]
    results = []
    for row in rows:
        if not article_matches_csv(article, row["article"]):
            continue
        results.append({
            "source": source_name,
            "title": f"Арт. {row['article']}",
            "price": row["price"],
            "currency": "RUB",
            "url": f"http://saleponsse.ru/advanced_search_result.php?keywords={quote_plus(row['article'])}",
        })
    return results[:10] if results else [{"source": source_name, "error": "Ничего не найдено"}]


# ============================================================
# ЭНДПОИНТЫ
# ============================================================
@app.get("/api/search")
async def search(article: str = Query(..., min_length=2)):
    article_clean = article.strip()
    cache_key = normalize_article(article_clean)

    if cache_key in CACHE:
        ts, data = CACHE[cache_key]
        if time.time() - ts < CACHE_TTL:
            return {"article": article_clean, "cached": True, "results": data}

    r2022 = search_excel(price_2022, article_clean, "Прайс 2022")
    r2021 = search_excel(price_2021, article_clean, "Прайс 2021")
    rft   = search_excel(price_ft,   article_clean, "Forest Technologies")

    async with httpx.AsyncClient() as client:
        scraped = await asyncio.gather(
            scrape_saleponsse(client, article_clean),
            scrape_lpskomi(client, article_clean),
            return_exceptions=True,
        )

    all_results = []
    for r in scraped:
        if isinstance(r, Exception):
            all_results.append({"source": "unknown", "error": str(r)})
        else:
            all_results.extend(r)
    all_results.extend(r2022)
    all_results.extend(r2021)
    all_results.extend(rft)

    CACHE[cache_key] = (time.time(), all_results)
    return {"article": article_clean, "cached": False, "results": all_results}


@app.get("/api/status")
async def status():
    return {
        "lps_rows": len(lps_cache["rows"]),
        "price_2022_rows": len(price_2022["rows"]),
        "price_2021_rows": len(price_2021["rows"]),
        "price_ft_rows": len(price_ft["rows"]),
        "cached_articles": len(CACHE),
        "base_dir": get_base_dir(),
    }

@app.delete("/api/cache")
async def clear_cache():
    CACHE.clear()
    return {"status": "ok"}
