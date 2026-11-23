"""Refresh local CPI indexes and relative importance from official BLS sources."""
import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime, timezone
from tempfile import NamedTemporaryFile

import requests

from config import DB_PATH, ROOT
from .annual_weights import extract_weights

API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
TABLE_URL = "https://www.bls.gov/cpi/tables/relative-importance/{year}.htm"
CACHE = ROOT / ".bls-cache"
ALL_ITEMS = "CUUR0000SA0"
SHUTDOWN = "2025-10"
# November 2025 release, Table 2, also shows an unpublished NSA index.
# https://www.bls.gov/news.release/archives/cpi_12182025.htm
OMITTED_OBSERVATIONS = {("CUUR0000SETE", "2025-11")}
HISTORY_START = "1999-01"
WEIGHTS_START = "2024-01"


def connect(path=DB_PATH, mode="rw"):
    return sqlite3.connect(path.resolve().as_uri() + f"?mode={mode}", uri=True)


def month_range(start, end):
    return [f"{year:04d}-{month:02d}" for year in range(int(start[:4]), int(end[:4])+1)
            for month in range(1, 13) if start <= f"{year:04d}-{month:02d}" <= end]


def fetch(url, filename, payload=None, reuse=False):
    CACHE.mkdir(exist_ok=True)
    path = CACHE / filename
    if reuse and path.is_file():
        return path.read_text(encoding="utf-8")
    if payload:
        response = requests.post(url, json=payload, timeout=45)
    else:
        response = requests.get(url, timeout=45)
    response.raise_for_status()
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=CACHE, delete=False) as staged:
        staged.write(response.text)
    os.replace(staged.name, path)
    return response.text


def fetch_indexes(series, start, end, reuse):
    key = os.getenv("BLS_API_KEY")
    batch_size, year_limit = (50, 20) if key else (25, 10)
    values = {}
    for first in range(int(start[:4]), int(end[:4])+1, year_limit):
        last = min(first + year_limit - 1, int(end[:4]))
        for i in range(0, len(series), batch_size):
            batch = series[i:i+batch_size]
            payload = {"seriesid": batch, "startyear": str(first), "endyear": str(last)}
            digest = hashlib.sha256(json.dumps(payload).encode()).hexdigest()[:16]
            if key:
                payload["registrationkey"] = key
            result = json.loads(fetch(API_URL, f"indexes-{digest}.json", payload, reuse))
            if result.get("status") != "REQUEST_SUCCEEDED" or result.get("message"):
                raise ValueError(f"BLS API returned {result.get('status')} with messages; check cached response")
            records = result["Results"]["series"]
            if sorted(row["seriesID"] for row in records) != sorted(batch):
                raise ValueError("BLS response series do not match request")
            for row in records:
                sid = row["seriesID"]
                for item in row["data"]:
                    year, period = item["year"], item["period"]
                    if period == "M13":
                        continue
                    if not re.fullmatch(r"M(0[1-9]|1[0-2])", period) or not first <= int(year) <= last:
                        raise ValueError(f"Unexpected BLS period {year} {period}")
                    month = f"{year}-{period[1:]}"
                    if not start <= month <= end:
                        continue
                    if (sid, month) in values:
                        raise ValueError(f"Duplicate observation {sid} {month}")
                    if month == SHUTDOWN and item["value"] == "-" and any(
                        "lapse in appropriations" in (note.get("text") or "").lower()
                        for note in item.get("footnotes", [])
                    ):
                        values[sid, month] = None
                        continue
                    value = float(item["value"])
                    if not math.isfinite(value) or value <= 0:
                        raise ValueError(f"Invalid index {sid} {month}")
                    values[sid, month] = value
    for sid, month in OMITTED_OBSERVATIONS:
        if start <= month <= end:
            values.setdefault((sid, month), None)
    for month in month_range(start, end):
        absent = [sid for sid in series if (sid, month) not in values]
        if absent:
            raise ValueError(f"Incomplete response for {month}: {absent}")
    return values


def update_db(add_date_string, reuse=False):
    if not re.fullmatch(r"\d{4}-\d{2}-01", add_date_string):
        raise ValueError("Refresh date must have the form YYYY-MM-01")
    end = date.fromisoformat(add_date_string).isoformat()[:7]
    if end < WEIGHTS_START:
        raise ValueError(f"Refresh must end in {WEIGHTS_START} or later")
    with closing(connect()) as db:
        initial_version = db.execute("PRAGMA data_version").fetchone()[0]
        series = db.execute("SELECT series_id, series_desc, level, priority FROM series_relation ORDER BY priority").fetchall()
        if len(series) != 57 or len({row[0] for row in series}) != 57:
            raise ValueError("Expected the 57 original CPI series")
        ids = [row[0] for row in series]
        values = fetch_indexes(ids, HISTORY_START, end, reuse)

        anchors = {}
        for year in range(int(WEIGHTS_START[:4])-1, int(end[:4])):
            html = fetch(TABLE_URL.format(year=year), f"december-{year}.html", reuse=reuse)
            anchors[year] = extract_weights(html, year, series)

        index_rows, weight_rows = [], []
        gaps = []
        for month in month_range(HISTORY_START, end):
            year, number = map(int, month.split("-"))
            previous_december = f"{year-1}-12"
            all_items = values[ALL_ITEMS, month]
            for sid, description, level, priority in series:
                current = values[sid, month]
                if current is None:
                    gaps.append((sid, month))
                    continue
                meta = (year, number, sid, description, level, priority)
                index_rows.append((*meta, current, month + "-01"))
                if month >= WEIGHTS_START and all_items is not None:
                    ratio = (current / values[sid, previous_december]) / (
                        all_items / values[ALL_ITEMS, previous_december])
                    weight = round(anchors[year-1][sid] * ratio, 3)
                    weight_rows.append((*meta, weight, month + "-01"))

        # Lock writers before taking the backup and replacing the validated range.
        db.execute("BEGIN IMMEDIATE")
        try:
            if db.execute("PRAGMA data_version").fetchone()[0] != initial_version:
                raise ValueError("SQLite changed during refresh; retry with current data")
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            backup_dir = ROOT / ".db-backups"
            backup_dir.mkdir(exist_ok=True)
            backup = backup_dir / f"pre-refresh-{stamp}.db"
            with closing(connect(mode="ro")) as source, closing(sqlite3.connect(backup)) as target:
                source.backup(target)
            for table, column, rows, start in (
                ("inflation_index", "value", index_rows, HISTORY_START),
                ("weights_table", "weight", weight_rows, WEIGHTS_START),
            ):
                db.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_series_date ON {table}(series_id,date)")
                db.execute(f"DELETE FROM {table} WHERE date BETWEEN ? AND ?", (start + "-01", add_date_string))
                db.executemany(f"""INSERT INTO {table}
                    (year,month,series_id,series_desc,level,priority,{column},date)
                    VALUES (?,?,?,?,?,?,?,?)""", rows)
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS base_weight_series_year ON base_weight(series_id,year)")
            db.executemany("""INSERT INTO base_weight(series_desc,series_id,base_weight,year)
                VALUES (?,?,?,?) ON CONFLICT(series_id,year) DO UPDATE SET
                series_desc=excluded.series_desc,base_weight=excluded.base_weight""",
                [(desc, sid, weights[sid], year) for year, weights in anchors.items()
                 for sid, desc, *_ in series])
            db.commit()
        except BaseException:
            db.rollback()
            raise

    return {"through": end, "indexes": len(index_rows), "weights": len(weight_rows),
            "official_gaps": len(gaps), "backup": str(backup)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("through", help="Latest released month, YYYY-MM")
    parser.add_argument("--reuse-cache", action="store_true")
    args = parser.parse_args()
    print(json.dumps(update_db(args.through + "-01", args.reuse_cache), indent=2))
