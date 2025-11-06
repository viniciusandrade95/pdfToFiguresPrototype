# llm_helpers.py
import os, sys, re, json, time, requests
from typing import Any, Dict, List, Optional

# ----------------------------- USER SETTINGS -----------------------------
API_KEY  = "sk-97DFSFSDFFDSFSSDDSFSFSFDS"  # set env var or put key here
BASE_URL = "https://llm.lab.sspcloud.fr/api"
TIMEOUT  = 30
HEADERS  = {"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"}
RETRIES  = 3
BACKOFF  = 4

# ----------------------------- Model probing -----------------------------
def _numeric_param_size(txt: str) -> float:
    """
    "120B" -> 120.0 ; "566.7M" -> 0.5667 (billions)
    Accepts M or B. Unknown -> 0.0
    """
    m = re.fullmatch(r"([\d.]+)([MB])", txt.strip().upper())
    if not m:
        return 0.0
    val, unit = m.groups()
    v = float(val)
    return v/1000.0 if unit == "M" else v

def get_most_powerful_model(default: str = "gpt-oss:20b") -> str:
    try:
        r = requests.get(f"{BASE_URL}/models", headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json().get("data", [])
        best_id, best_val = None, -1.0
        for m in data:
            size = (
                m.get("info", {}).get("params", {}).get("parameter_size") or
                m.get("ollama", {}).get("details", {}).get("parameter_size")
            )
            if size:
                num = _numeric_param_size(size)
                if num > best_val:
                    best_val, best_id = num, m["id"]
        return best_id or default
    except Exception as e:
        print(f"[LLM] model list failed: {e}", file=sys.stderr)
        return default

# ----------------------------- Core JSON chat -----------------------------
def _post_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(1, RETRIES+1):
        for ep in ("/chat/completions", "/v1/chat/completions"):
            try:
                r = requests.post(f"{BASE_URL}{ep}", headers=HEADERS, json=payload, timeout=TIMEOUT)
                if r.status_code == 200:
                    return r.json()
            except requests.RequestException:
                pass
        if attempt < RETRIES:
            time.sleep(BACKOFF * attempt)
    raise RuntimeError("LLM call failed after retries")

def llm_chat_json(system_prompt: str, user_prompt: str, schema_hint: str, max_tokens: int = 400) -> Dict[str, Any]:
    """
    Ask LLM to return STRICT JSON. If the model adds prose, extract the JSON object/array.
    """
    model = "gpt-oss:20b"
    payload = {
        "model": "gpt-oss:20b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": f"{user_prompt}\n\nReturn STRICT JSON only. JSON schema:\n{schema_hint}"}
        ],
        "temperature": 0.0,
        "max_tokens": max_tokens,
    }
    resp = _post_json(payload)
    raw = resp["choices"][0]["message"]["content"].strip()
    m = re.search(r"\{.*\}|\[.*\]", raw, flags=re.S)
    txt = m.group(0) if m else raw
    return json.loads(txt)

# ----------------------------- 1) Table classifier -----------------------------
def classify_table(snippet: str) -> Dict[str, Any]:
    """
    Output: { is_financial_table: bool, section: str, language: str, unit_hint: str|null }
    """
    system = "You are a careful classifier of tables in annual reports."
    schema = '{"is_financial_table": true, "section": "income_statement|balance_sheet|cash_flow|financials|operating|esg|other", "language": "en|pl|de|fr|es|it|pt|nl|...", "unit_hint": "string|null"}'
    user = (
        "Decide if this is a financial table (not prose). "
        "If yes, choose section and language. If you see units like '£m', '€k', 'w tys. zł', add them as unit_hint.\n\n"
        f"SNIPPET:\n{snippet[:1500]}"
    )
    try:
        out = llm_chat_json(system, user, schema_hint=schema)
    except Exception:
        out = {"is_financial_table": False, "section": "other", "language": "en", "unit_hint": None}
    return out

# ----------------------------- 2) Units & value types resolver -----------------------------
def infer_units(context: str, header_row: str) -> Dict[str, Any]:
    """
    Output: {currency: "...", scale: 1|1000|1000000|1000000000,
             columns: [{"col": <int>, "value_type": "money|percent|bps|count|per_share_pence|per_share_cent|ratio"}]}
    """
    system = "You infer units and value types for columns in financial tables."
    schema = '{"currency":"string|null","scale":1,"columns":[{"col":1,"value_type":"money|percent|bps|count|per_share_pence|per_share_cent|ratio"}]}'
    user = (
        "Given the page context (with unit cues) and the first header row text, infer currency and global scale, "
        "and the value_type per numeric column. Use GBP/EUR/USD/PLN where possible. "
        "Examples of value_type: money, percent, bps, count, per_share_pence, per_share_cent, ratio.\n\n"
        f"CONTEXT:\n{context[:1200]}\n\nHEADER_ROW:\n{header_row[:400]}"
    )
    try:
        out = llm_chat_json(system, user, schema_hint=schema)
    except Exception:
        out = {"currency": None, "scale": 1, "columns": []}
    return out

# ----------------------------- 3) Metric normalizer (batch) -----------------------------
def map_metrics(unique_labels: List[str], section_hint: Optional[str] = None, language: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Output: [{"raw":"Revenue (exc. VAT, inc. fuel)","canonical":"total_revenue"}, ...]
    """
    system = "You normalize metric labels to canonical financial keys used in BI systems."
    schema = '[{"raw":"string","canonical":"string"}]'
    examples_dict = (
        "Allowed canonical keys include: total_revenue, cost_of_sales, gross_profit, operating_costs, "
        "operating_profit, ebit, ebitda, net_finance_costs, profit_before_tax, net_income, "
        "total_assets, total_liabilities, total_equity, net_debt, cash_and_cash_equivalents, "
        "capex, free_cash_flow, dividend_per_share, basic_eps, diluted_eps, roe, roa, margin, "
        "lcr, nsfr, nim, npl_ratio, cost_of_risk."
    )
    user = (
        f"Map the following raw metric labels to canonical keys. If unknown, use 'other'.\n"
        f"Section hint: {section_hint or 'unknown'} | Language: {language or 'unknown'}\n"
        f"Canonical dictionary (examples): {examples_dict}\n\n"
        f"RAW LABELS:\n" + "\n".join(f"- {x}" for x in unique_labels[:200])
    )
    try:
        out = llm_chat_json(system, user, schema_hint=schema, max_tokens=800)
    except Exception:
        out = [{"raw": x, "canonical": "other"} for x in unique_labels]
    return out
