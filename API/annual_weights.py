"""Parse the new-weight CPI-U column of BLS December Table 1."""
import csv
import re
from html.parser import HTMLParser

from config import ROOT


class Table(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.captions = []
        self.field = None
        self.row = []

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        if tag in ("th", "td", "caption"):
            self.field = []
        if tag in ("sup", "br") and self.field is not None:
            self.field.append(" ")

    def handle_data(self, data):
        if self.field is not None:
            self.field.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td", "caption") and self.field is not None:
            value = " ".join("".join(self.field).split())
            (self.captions if tag == "caption" else self.row).append(value)
            self.field = None
        if tag == "tr" and self.row:
            self.rows.append(self.row)


def normalize(label):
    return re.sub(r"\s*\(\d+\)", "", label).strip().casefold()


def read_rows(html, year):
    table = Table()
    table.feed(html)
    caption = f"Table 1 ({year-1} Weights)."
    if not any(c.startswith(caption) and f"December {year}" in c for c in table.captions):
        raise ValueError(f"Unexpected December {year} table caption: {table.captions}")
    if not any(r[:2] == ["Item and Group", "U.S. City Average, CPI-U"] for r in table.rows):
        raise ValueError("Expected U.S. city average CPI-U in second column")
    return [(normalize(r[0]), float(r[1])) for r in table.rows
            if len(r) == 3 and re.fullmatch(r"\d+\.\d+", r[1])]


def extract_weights(html, year, series):
    rows = read_rows(html, year)
    with (ROOT / "datafiles/bls_weight_labels.csv").open(newline="", encoding="utf-8") as file:
        mappings = list(csv.DictReader(file))
    labels = {record["series_id"]: normalize(record["bls_label"]) for record in mappings}
    if len(labels) != len(mappings) or labels.keys() != {record[0] for record in series}:
        raise ValueError("Expected exactly one Table 1 mapping for each CPI series")
    if len(set(labels.values())) != len(labels):
        raise ValueError("Table 1 labels must be distinct")
    weights = {}
    for sid, label in labels.items():
        matches = [weight for name, weight in rows if name == label]
        # All items appears once in the main table and again under special aggregates.
        if sid == "CUUR0000SA0" and matches == [100, 100]:
            matches = matches[:1]
        if len(matches) != 1 or not 0 < matches[0] <= 100:
            raise ValueError(f"Expected one usable weight for {sid} ({label}) in {year}")
        weights[sid] = matches[0]
    if weights["CUUR0000SA0"] != 100:
        raise ValueError("All-items weight must be 100")
    major_total = sum(weights[sid] for sid, _, level, _ in series if level == 1)
    if abs(major_total - 100) > 0.01:
        raise ValueError(f"Major category weights for {year} sum to {major_total}, expected 100")
    return weights
