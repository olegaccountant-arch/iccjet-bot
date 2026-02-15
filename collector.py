import os
import time
import hashlib
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

DB_PATH = "data/seen.sqlite"
REPORT_PATH = "data/latest_report.md"


@dataclass
class Listing:
    source: str
    title: str
    url: str
    year: Optional[str] = None
    price: Optional[str] = None
    location: Optional[str] = None


def ensure_dirs():
    os.makedirs("data", exist_ok=True)


def fp(it: Listing) -> str:
    return hashlib.sha1((it.source + "|" + it.url).encode("utf-8")).hexdigest()


class Seen:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (fp TEXT PRIMARY KEY)")
        self.db.commit()

    def is_seen(self, f: str) -> bool:
        cur = self.db.execute("SELECT 1 FROM seen WHERE fp=?", (f,))
        return cur.fetchone() is not None

    def mark(self, f: str):
        self.db.execute("INSERT OR IGNORE INTO seen(fp) VALUES(?)", (f,))
        self.db.commit()


def fetch(url: str) -> str:
    """
    Делает 1-2 попытки скачать страницу.
    Если сайт блокирует (403/429/503) — кидаем RuntimeError, но main() поймает и просто пропустит источник.
    """
    last_err = None
    for _ in range(2):
        try:
            r = requests.get(
                url,
                timeout=30,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                    )
                },
            )
            if r.status_code >= 400:
                raise RuntimeError(f"{r.status_code} for {url}")
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise last_err


def dedupe_by_url(items: List[Listing]) -> List[Listing]:
    seen = set()
    out = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


# -----------------------------
# Источники
# -----------------------------

def controller_jets() -> List[Listing]:
    # Часто даёт 403 на GitHub Actions — это ок, main() это поймает.
    url = "https://www.controller.com/listings/for-sale/jet-aircraft/3"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Listing] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if "/listing/" in href and text and len(text) > 6:
            full = "https://www.controller.com" + href if href.startswith("/") else href
            out.append(Listing(source="Controller", title=text, url=full))

    return dedupe_by_url(out)


def avbuyer_jets() -> List[Listing]:
    url = "https://www.avbuyer.com/aircraft/private-jets"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Listing] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not href or not text:
            continue

        # Пытаемся выцепить ссылки, похожие на карточки объявлений
        if "/aircraft/" in href and ("jet" in href or "jets" in href or "private" in href):
            full = "https://www.avbuyer.com" + href if href.startswith("/") else href
            if len(text) > 6:
                out.append(Listing(source="AvBuyer", title=text, url=full))

    return dedupe_by_url(out)


def globalair_jets() -> List[Listing]:
    url = "https://www.globalair.com/aircraft-for-sale/private-jet"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Listing] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not href or not text:
            continue

        if "aircraft-for-sale" in href or "/aircraft/" in href:
            full = "https://www.globalair.com" + href if href.startswith("/") else href
            if len(text) > 6:
                out.append(Listing(source="GlobalAir", title=text, url=full))

    return dedupe_by_url(out)


def aeroclassifieds_jets() -> List[Listing]:
    url = "https://www.aeroclassifieds.com/"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Listing] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not href or not text:
            continue

        if "aircraft" in href or "listing" in href:
            full = "https://www.aeroclassifieds.com" + href if href.startswith("/") else href
            if len(text) > 6:
                out.append(Listing(source="AeroClassifieds", title=text, url=full))

    return dedupe_by_url(out)


def jamesedition_jets() -> List[Listing]:
    # Может быть динамический/антибот. Если заблокируют — main() поймает.
    url = "https://www.jamesedition.com/jets"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Listing] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not href or not text:
            continue

        if "/jets/" in href or "jet" in href:
            full = "https://www.jamesedition.com" + href if href.startswith("/") else href
            if len(text) > 6:
                out.append(Listing(source="JamesEdition", title=text, url=full))

    return dedupe_by_url(out)


def txtav_preowned_jets() -> List[Listing]:
    url = "https://www.txtav.com/en/pre-owned"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Listing] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not href or not text:
            continue

        if "pre-owned" in href or "preowned" in href:
            full = "https://www.txtav.com" + href if href.startswith("/") else href
            if len(text) > 6:
                out.append(Listing(source="Textron", title=text, url=full))

    return dedupe_by_url(out)


# -----------------------------
# Фильтр "оставлять только джеты"
# -----------------------------

JET_KEYWORDS = [
    # бизнес-джеты / производители / серии
    "gulfstream", "falcon", "global", "challenger", "citation",
    "legacy", "praetor", "learjet", "hawker", "embraer",
    "bombardier", "cessna", "dassault",

    # VIP airliners / конверсии
    "bbj", "acj", "lineage", "airbus", "boeing",
]


def is_jet(it: Listing) -> bool:
    t = (it.title or "").lower()
    return any(k in t for k in JET_KEYWORDS)


# -----------------------------
# Отчёт
# -----------------------------

def make_report(new_items: List[Listing], failed_sources: List[Tuple[str, str]]) -> str:
    lines = []
    lines.append("# Daily Business Jet Listings")
    lines.append("")
    lines.append(f"Новых за сегодня: **{len(new_items)}**")
    lines.append("")

    if failed_sources:
        lines.append("## Источники с ошибкой (не критично)")
        for name, err in failed_sources[:50]:
            lines.append(f"- {name}: {err[:180]}")
        lines.append("")

    lines.append("## Новые объявления")
    lines.append("")
    if not new_items:
        lines.append("Сегодня новых объявлений не найдено (или источники были заблокированы).")
        lines.append("")

    for it in new_items[:250]:
        meta = " | ".join([x for x in [it.year, it.price, it.location] if x])
        lines.append(f"- **{it.title}**")
        lines.append(f"  - Source: {it.source}")
        if meta:
            lines.append(f"  - {meta}")
        lines.append(f"  - Link: {it.url}")
        lines.append("")
    return "\n".join(lines)


# -----------------------------
# Главная функция
# -----------------------------

def main():
    ensure_dirs()
    seen = Seen(DB_PATH)

    adapters = [
        ("Controller", controller_jets),
        ("AvBuyer", avbuyer_jets),
        ("GlobalAir", globalair_jets),
        ("AeroClassifieds", aeroclassifieds_jets),
        ("JamesEdition", jamesedition_jets),
        ("Textron Pre-Owned", txtav_preowned_jets),
    ]

    items: List[Listing] = []
    failed: List[Tuple[str, str]] = []

    for name, fn in adapters:
        try:
            got = fn()
            items += got
            print(f"[OK] {name}: {len(got)} items")
        except Exception as e:
            failed.append((name, str(e)))
            print(f"[WARN] {name} failed: {e}")

        time.sleep(0.5)  # небольшой перерыв между источниками

    items = [x for x in items if is_jet(x)]

    new_items: List[Listing] = []
    for it in items:
        f = fp(it)
        if not seen.is_seen(f):
            new_items.append(it)
            seen.mark(f)

    report = make_report(new_items, failed)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)


if __name__ == "__main__":
    main()
