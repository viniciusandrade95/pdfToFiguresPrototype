#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extractor Quant v3 — Robust, table-first financial extractor for annual reports (PDF → tidy facts)

Design goals
------------
- Prioritise **quantitative accuracy** over cleverness. No LLMs required.
- Be permissive in **table gating** so we never miss clean statement tables.
- Handle multi‑year headers (e.g., "Mar 31, 2025" / "2025" / "FY 24/25") and simple 2‑column KPI tables.
- Detect currency + scale from page/table cues (e.g., "€'m", "EUR million", "€m", "$ bn").
- Normalise to a long format table suitable for analysis and wide pivots.

Key outputs
-----------
- SQLite table: financial_facts
- CSV/JSON exports: <out>_facts_long.csv, <out>_facts_wide.csv

CLI
---
python extractor_quant_v3.py \
  --reports ./reports \
  --glob "*.pdf" \
  --db facts.sqlite \
  --ocr 0 \
  --out run1

Notes
-----
- Requires: camelot-py[cv], PyMuPDF (fitz), pandas, numpy, Pillow (if OCR), pytesseract (optional)
- OCR is only used to enrich page text for currency/section cues, not for table structure.
"""

from __future__ import annotations
import re, io, os, json, argparse, logging, sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

import fitz  # PyMuPDF
import camelot
from PIL import Image

# Optional OCR (only if --ocr 1)
try:
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

log = logging.getLogger("extractor_quant_v3")
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ------------------------- Config -------------------------
SECTION_PATTERNS = {
    "income_statement": [
        r"\bconsolidated\s+statement\s+of\s+(profit|loss|income)\b",
        r"\bstatement\s+of\s+operations\b",
        r"\bprofit\s+or\s+loss\b",
        r"\bincome\s+statement\b",
    ],
    "balance_sheet": [
        r"\bconsolidated\s+statement\s+of\s+financial\s+position\b",
        r"\b(consolidated\s+)?balance\s+sheet\b",
        r"\bstatement\s+of\s+financial\s+position\b",
    ],
    "cash_flow": [
        r"\bconsolidated\s+statement\s+of\s+cash\s+flows?\b",
        r"\bcash\s+flow\s+statement\b",
    ],
    "kpi": [
        r"\bkey\s+metrics?\b",
        r"\bkey\s+performance\s+indicators?\b",
        r"\bhighlights?\b",
        r"\bfinancial\s+summary\b",
    ],
}

CURRENCY_SYMBOLS = {
    "€": "EUR",
    "$": "USD",
    "£": "GBP",
    "¥": "JPY",
}

CURRENCY_WORDS = {
    r"\bEUR\b": "EUR",
    r"\bEURO(S)?\b": "EUR",
    r"\bUSD\b": "USD",
    r"\bUS\s*DOLLAR(S)?\b": "USD",
    r"\bGBP\b": "GBP",
    r"\bPOUND(S)?\b": "GBP",
}

SCALE_KEYWORDS = [
    (r"\b(thousands|thousand|000s|k)\b", "×1k", 1e3),
    (r"\b(million|millions|mn|m)\b", "×1m", 1e6),
    (r"\b(billion|billions|bn|b)\b", "×1b", 1e9),
]

MIN_YEAR, MAX_YEAR = 1995, 2036

# ------------------------- Utils -------------------------

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    # common PDF whitespace/quotes
    s = s.replace("\u2009", " ").replace("\xa0", " ").replace("\u202f", " ")
    s = s.replace("’", "'")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def infer_company_and_year(path: Path) -> Tuple[str, Optional[int]]:
    stem = path.stem
    for sep in ("_", "-", " "):
        if sep in stem:
            a, b = stem.split(sep, 1)
            if a.isdigit() and len(a) == 4:
                return b, int(a)
            if b.isdigit() and len(b) == 4:
                return a, int(b)
    return stem, None


def page_text_of(page: fitz.Page, ocr: bool = False, dpi: int = 220) -> str:
    txt = page.get_text("text") or ""
    txt = normalize_text(txt)
    if len(txt) >= 50 or not ocr:
        return txt
    if not OCR_AVAILABLE:
        return txt
    # Fallback OCR for short text pages
    try:
        pix = page.get_pixmap(dpi=dpi)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        ocr_txt = pytesseract.image_to_string(img)
        return normalize_text(ocr_txt) or txt
    except Exception as exc:
        log.warning(f"OCR failed on p{page.number+1}: {exc}")
        return txt


def detect_section_label(text: str) -> Optional[str]:
    t = text.lower()
    for section, patterns in SECTION_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, t, flags=re.I):
                return section
    return None


def contains_currency_symbol(text: str) -> bool:
    return any(sym in text for sym in CURRENCY_SYMBOLS)


def detect_currency_and_scale(text: str) -> Tuple[Optional[str], Optional[str], float]:
    """Find currency (symbol/word) + scale (k/m/b) from text like page header/caption."""
    currency = None
    unit_raw = None
    scale = 1.0

    # by symbol
    for sym, code in CURRENCY_SYMBOLS.items():
        if sym in text:
            currency = code
            break

    # by words
    if not currency:
        for pat, code in CURRENCY_WORDS.items():
            if re.search(pat, text, re.I):
                currency = code
                break

    # compact patterns (symbol + scale), e.g. €m, $bn
    m2 = re.search(r"([€$£¥])\s*(m|mn|bn|k|b)\b", text, re.I)
    if m2:
        word = m2.group(2).lower()
        if word in ("m", "mn"): unit_raw, scale = "×1m", 1e6
        elif word == "bn" or word == "b": unit_raw, scale = "×1b", 1e9
        elif word == "k": unit_raw, scale = "×1k", 1e3
        if not currency:
            currency = CURRENCY_SYMBOLS.get(m2.group(1))

    # word + scale e.g. EUR million, USD thousand
    m3 = re.search(r"\b(EUR|USD|GBP|JPY)\b\s*(million|millions|mn|m|billion|billions|bn|b|thousand|thousands|k)\b", text, re.I)
    if m3:
        currency = m3.group(1).upper()
        word = m3.group(2).lower()
        for pat, unit, sc in SCALE_KEYWORDS:
            if re.fullmatch(pat, word):
                unit_raw, scale = unit, sc
                break
        # direct map if regex fullmatch didn't hit due to group
        if unit_raw is None:
            if word in ("million","millions","mn","m"): unit_raw, scale = "×1m", 1e6
            elif word in ("billion","billions","bn","b"): unit_raw, scale = "×1b", 1e9
            elif word in ("thousand","thousands","k"): unit_raw, scale = "×1k", 1e3

    # picky header like €'m or $'m
    if contains_currency_symbol(text) and re.search(r"['’]m\b", text, re.I):
        unit_raw, scale = "×1m", 1e6

    return currency, unit_raw, scale


# ------------------------- Table helpers -------------------------

def looks_like_year(token: str) -> Optional[int]:
    tok = token.strip().strip("'\u2019")
    # absolute year
    m = re.fullmatch(r"(19|20)\d{2}", tok)
    if m:
        y = int(tok)
        return y if MIN_YEAR <= y <= MAX_YEAR else None
    # FY 24/25 → map to 2025
    m = re.search(r"FY\s*(\d{2})\s*/\s*(\d{2})", tok, re.I)
    if m:
        y = int("20" + m.group(2))
        return y if MIN_YEAR <= y <= MAX_YEAR else None
    # month + day + year patterns (Mar 31, 2025)
    m = re.search(r"\b(19|20)\d{2}\b", tok)
    if m:
        y = int(m.group(0))
        return y if MIN_YEAR <= y <= MAX_YEAR else None
    return None


def find_year_header_map(df: pd.DataFrame) -> Tuple[Optional[int], Dict[int, int]]:
    """Return (header_row_index, {col_index: year}) when a row contains ≥2 distinct years."""
    for i in range(len(df)):
        years: Dict[int, int] = {}
        for j in range(len(df.columns)):
            cell = normalize_text(str(df.iat[i, j]))
            # split by non-alnum to find sub tokens like "Mar 31, 2025"
            tokens = re.split(r"[^\w'\-]+", cell)
            y_found = None
            for tok in tokens:
                y = looks_like_year(tok)
                if y:
                    y_found = y
                    break
            if y_found:
                years[j] = y_found
        if len(years) >= 2:
            return i, years
    return None, {}


def clean_numeric(raw: str) -> Optional[float]:
    if raw is None:
        return None
    s = normalize_text(raw)
    if s in ("", "-", "–", "—"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    s = re.sub(r"[^\d,.-]", "", s)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    s = s.replace(",", "")
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None


def numeric_density(df: pd.DataFrame) -> float:
    if df.size == 0:
        return 0.0
    cnt = 0
    for _, row in df.iterrows():
        for cell in row:
            if re.search(r"\d", str(cell)):
                cnt += 1
    return cnt / df.size

# ------------------------- DB -------------------------

def init_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT,
            fiscal_year INTEGER,
            section TEXT,
            metric TEXT,
            value REAL,
            currency TEXT,
            unit_raw TEXT,
            scale_applied REAL,
            source_page INTEGER,
            source_type TEXT,
            confidence REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS u_facts
        ON financial_facts(company, fiscal_year, section, metric, source_page, source_type)
        """
    )
    db.commit()
    return db


def insert_fact(cur: sqlite3.Cursor,
                company: str,
                fy: Optional[int],
                section: Optional[str],
                metric: str,
                value: Optional[float],
                currency: Optional[str],
                unit_raw: Optional[str],
                scale_applied: float,
                page_no: int,
                source_type: str,
                confidence: float):
    cur.execute(
        """
        INSERT OR REPLACE INTO financial_facts
        (company, fiscal_year, section, metric, value, currency, unit_raw, scale_applied, source_page, source_type, confidence)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (company, fy, section, metric, value, currency, unit_raw, scale_applied, page_no, source_type, confidence)
    )

# ------------------------- Core extraction -------------------------

def parse_table(df: pd.DataFrame,
                company: str,
                default_year: Optional[int],
                page_no: int,
                page_text: str,
                source_type: str,
                cur: sqlite3.Cursor,
                section_hint: Optional[str] = None):
    # page cues
    currency_p, unit_p, scale_p = detect_currency_and_scale(page_text)

    # normalise df
    df = df.copy()
    df.columns = [normalize_text(str(c)) for c in df.columns]

    header_idx, col2year = find_year_header_map(df)

    # permissive gating: accept if it has multiple years OR has reasonable numeric density
    dens = numeric_density(df)
    if header_idx is None and dens < 0.12:
        return  # too sparse

    # section from page text
    section = section_hint or detect_section_label(page_text)

    # try to detect currency/scale from the first few rows (headers/captions)
    head_text = " ".join([" ".join(map(str, df.iloc[i].tolist())) for i in range(min(3, len(df)))])
    currency_t, unit_t, scale_t = detect_currency_and_scale(head_text)

    currency = currency_t or currency_p
    unit_raw = unit_t or unit_p
    scale_applied = scale_t or scale_p or 1.0

    # 1) Multi-year
    if header_idx is not None and len(col2year) >= 2:
        data_df = df.iloc[header_idx + 1:].reset_index(drop=True)
        for i in range(len(data_df)):
            metric = normalize_text(str(data_df.iat[i, 0])).lower()
            if not metric or metric in ("-", "—"):
                continue
            for j, year in col2year.items():
                if j >= len(data_df.columns):
                    continue
                val = clean_numeric(data_df.iat[i, j])
                if val is None:
                    continue
                insert_fact(cur, company, year, section, metric, val * scale_applied,
                            currency, unit_raw, scale_applied, page_no, source_type, 1.0)
        return

    # 2) Fallback: KPI / 2‑column (metric | value)
    # pick the first numeric in the row after column 0
    for i in range(len(df)):
        metric = normalize_text(str(df.iat[i, 0])).lower()
        if not metric:
            continue
        val = None
        for j in range(1, len(df.columns)):
            val = clean_numeric(df.iat[i, j])
            if val is not None:
                break
        if val is None:
            continue
        # year fallback: try to sniff any single year on the page
        fy = default_year
        m = re.search(r"\b(19|20)\d{2}\b", page_text)
        if fy is None and m:
            y = int(m.group(0))
            if MIN_YEAR <= y <= MAX_YEAR:
                fy = y
        insert_fact(cur, company, fy, section, metric, val * scale_applied,
                    currency, unit_raw, scale_applied, page_no, source_type, 1.0)


def extract_from_pdf(pdf_path: Path, db: sqlite3.Connection, ocr_fallback: bool = False):
    company, default_year = infer_company_and_year(pdf_path)
    log.info(f"Processing: {pdf_path.name} | company='{company}' year='{default_year}'")

    doc = fitz.open(str(pdf_path))
    cur = db.cursor()

    for page in tqdm(doc, desc=f"{pdf_path.name}"):
        page_no = page.number + 1
        page_text = page_text_of(page, ocr=ocr_fallback)

        # section hint
        section_hint = detect_section_label(page_text)

        try:
            tables = camelot.read_pdf(
                str(pdf_path),
                pages=str(page_no),
                flavor="stream",
                edge_tol=500,
                strip_text='\n'
            )
        except Exception as exc:
            log.warning(f"Camelot error on page {page_no}: {exc}")
            continue

        if tables.n == 0:
            continue

        for t in tables:
            df = t.df
            if df is None or df.empty:
                continue
            try:
                parse_table(df, company, default_year, page_no, page_text,
                            source_type=("camelot_ocr" if ocr_fallback else "camelot_text"),
                            cur=cur, section_hint=section_hint)
            except Exception as exc:
                log.warning(f"Parse error p{page_no}: {exc}")
        db.commit()

    doc.close()

# ------------------------- Exporters -------------------------

def export_all(db: sqlite3.Connection, out_prefix: str):
    df = pd.read_sql_query("SELECT * FROM financial_facts", db)
    if df.empty:
        log.warning("No rows extracted; skipping exports.")
        return

    # Long
    long_csv = f"{out_prefix}_facts_long.csv"
    long_json = f"{out_prefix}_facts_long.json"
    df.to_csv(long_csv, index=False)
    df.to_json(long_json, orient="records")

    # Wide convenience
    try:
        pivot = (df
                 .pivot_table(index=["company", "fiscal_year", "section"],
                              columns="metric", values="value", aggfunc="first")
                 .reset_index())
        pivot.columns = [c if isinstance(c, str) else c[1] for c in pivot.columns]
        wide_csv = f"{out_prefix}_facts_wide.csv"
        pivot.to_csv(wide_csv, index=False)
        log.info(f"Exported: {long_csv}, {long_json}, {wide_csv}")
    except Exception as exc:
        log.info(f"Exported: {long_csv}, {long_json}")
        log.warning(f"Wide export failed (non-fatal): {exc}")

# ------------------------- CLI -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extractor Quant v3 — robust table-first financial extractor")
    p.add_argument("--reports", type=str, required=True, help="Folder with PDFs")
    p.add_argument("--glob", type=str, default="*.pdf", help="File glob (default: *.pdf)")
    p.add_argument("--db", type=str, default="financial_facts.sqlite", help="SQLite DB path")
    p.add_argument("--ocr", type=int, default=0, help="Enable OCR fallback per page (0/1)")
    p.add_argument("--out", type=str, default="output", help="Export prefix for CSV/JSON")
    return p.parse_args()


def main():
    args = parse_args()
    reports_dir = Path(args.reports)
    if not reports_dir.exists():
        log.error(f"Reports folder not found: {reports_dir}")
        return

    db = init_db(args.db)

    pdfs = sorted(reports_dir.glob(args.glob))
    if not pdfs:
        log.warning("No PDFs matched the given glob.")

    for pdf in pdfs:
        extract_from_pdf(pdf, db, ocr_fallback=bool(args.ocr))

    export_all(db, args.out)


if __name__ == "__main__":
    main()
