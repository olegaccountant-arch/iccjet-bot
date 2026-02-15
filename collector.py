import os
import time
import hashlib
import sqlite3
from dataclasses import dataclass
from typing import List, Optional

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
    r = requests.get(url, timeout=30, headers={"User-Agent": "ICCJetBot/1.0"})
    r.raise_for_status()
    return r.text

# --- ПРИМЕР: 1 источник (Controller). Потом добавим остальные ---
def controller_jets() -> List[Listing]:
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

    return out

# Фильтр "оставлять только джеты" (простая версия по ключевым словам)
JET_KEYWORDS = [
    "gulfstream", "falcon", "global", "challenger", "citation",
    "legacy", "praetor", "learjet", "hawker", "embraer",
    "bbj", "acj", "lineage", "airbus", "boeing"
]

def is_jet(it: Listing) -> bool:
    t = it.title.lower()
    return any(k in t for k in JET_KEYWORDS)

def make_report(new_items: List[Listing]) -> str:
    lines = []
    lines.append("# Daily Business Jet Listings")
    lines.append("")
    lines.append(f"Новых за сегодня: **{len(new_items)}**")
    lines.append("")
    for it in new_items[:200]:
        meta = " | ".join([x for x in [it.year, it.price, it.location] if x])
        lines.append(f"- **{it.title}**")
        lines.append(f"  - Source: {it.source}")
        if meta:
            lines.append(f"  - {meta}")
        lines.append(f"  - Link: {it.url}")
        lines.append("")
    return "\n".join(lines)

def main():
    ensure_dirs()
    seen = Seen(DB_PATH)

    items = []
    # пока один источник, потом добавим остальные
    items += controller_jets()

    items = [x for x in items if is_jet(x)]

    new_items: List[Listing] = []
    for it in items:
        f = fp(it)
        if not seen.is_seen(f):
            new_items.append(it)
            seen.mark(f)
        time.sleep(0.15)

    report = make_report(new_items)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

if __name__ == "__main__":
    main()
