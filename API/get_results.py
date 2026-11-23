import re
from contextlib import closing
from datetime import date

from .update_data import ALL_ITEMS, connect, month_range

METRICS = {"Level": 0, "Monthly Rate": 1, "Annual Rate": 12}


def parse_month(year, month):
    if not re.fullmatch(r"\d{4}", year) or not re.fullmatch(r"0?[1-9]|1[0-2]", month):
        raise ValueError("Enter a four-digit year and a calendar month")
    result = date(int(year), int(month), 1)
    if result.year < 1999:
        raise ValueError("The dashboard begins in January 1999")
    return result.isoformat()


def shift(month, offset):
    year, number = divmod(int(month[:4])*12 + int(month[5:7])-1 + offset, 12)
    return f"{year:04d}-{number+1:02d}-01"


def percent(current, previous):
    if current is not None and previous is not None and previous > 0:
        return 100 * (current / previous - 1)
    return None


def time_series(db, args, start, end, series):
    if args.seriesType not in METRICS:
        raise ValueError("Unknown metric")
    if not args.seriesIds or len(set(args.seriesIds)) != len(args.seriesIds):
        raise ValueError("Choose one or more distinct series")
    descriptions = dict(series)
    if set(args.seriesIds) - descriptions.keys():
        raise ValueError("Unknown series ID")
    offset = METRICS[args.seriesType]
    placeholders = ",".join("?" for _ in args.seriesIds)
    values = {(sid, month): value for sid, month, value in db.execute(
        f"SELECT series_id,date,value FROM inflation_index WHERE date BETWEEN ? AND ? AND series_id IN ({placeholders})",
        (shift(start, -offset), end, *args.seriesIds))}
    dates = [month + "-01" for month in month_range(start[:7], end[:7])]
    data = {}
    for sid in args.seriesIds:
        points = []
        for month in dates:
            value = values.get((sid, month))
            if offset:
                value = percent(value, values.get((sid, shift(month, -offset))))
                if value is not None:
                    value = round(value, 2)
            points.append(value)
        data[sid] = {"series_desc": descriptions[sid], "value": points}
    return {"x-axis": dates, "data": data}


def compare(db, start, end):
    rows = db.execute("""
        SELECT s.series_id, s.series_desc, s.level, s.parent_category_series_id,
               a.value, b.value, w.weight
        FROM series_relation s
        LEFT JOIN inflation_index a ON a.series_id=s.series_id AND a.date=?
        LEFT JOIN inflation_index b ON b.series_id=s.series_id AND b.date=?
        LEFT JOIN weights_table w ON w.series_id=s.series_id AND w.date=?
        ORDER BY s.priority
    """, (start, end, start))
    names, contributions, details = [], [], {}
    actual = None
    parents = {}
    for sid, name, level, parent, first, last, weight in rows:
        change = percent(last, first)
        contribution = round(change * weight / 100, 4) if change is not None and weight is not None else None
        if sid == ALL_ITEMS:
            actual = round(change, 4) if change is not None else None
        if level == 1:
            names.append(name)
            contributions.append(contribution)
            details[name] = {}
            parents[sid] = name
        elif parent in parents:
            details[parents[parent]][name] = {"level": level, "value": contribution}
            parents[sid] = parents[parent]
    approximate = round(sum(contributions), 4) if all(v is not None for v in contributions) else None
    return {"x-axis": names, "y-axis": [100, *contributions, None if approximate is None else round(100 + approximate, 4)],
            "approximateRate": approximate, "actualRate": actual, "details": details}


def get_final_data(request_type, input_args):
    args = input_args
    start = parse_month(args.yearStart, args.monthStart)
    end = parse_month(args.yearEnd, args.monthEnd)
    with closing(connect(mode="ro")) as db:
        latest = db.execute("SELECT max(date) FROM inflation_index").fetchone()[0]
        if start > end or end > latest:
            raise ValueError(f"Choose dates in order through {latest[:7]}")
        if request_type == "time-series":
            return time_series(db, args, start, end, db.execute("SELECT series_id,series_desc FROM series_relation"))
        return compare(db, start, end)
