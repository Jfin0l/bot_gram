import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/p2p_data.db")


def _ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            exchange TEXT NOT NULL,
            fiat TEXT NOT NULL,
            trade_type TEXT NOT NULL,
            raw_json TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    # snapshots table
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            pair TEXT NOT NULL,
            rows_fetched INTEGER,
            avg_price_simple REAL,
            avg_price_weighted REAL,
            spread_pct REAL,
            coef_var REAL,
            total_exposed_volume REAL,
            top1_price REAL,
            top1_vol REAL,
            top1_nick TEXT,
            top3_prices TEXT,
            arb_estimate_cop_to_ves_pct REAL,
            arb_estimate_ves_to_cop_pct REAL,
            raw_json TEXT
        )
        """
    )
    conn.commit()
    conn.close()

    # Indexes to speed common queries
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_pair ON snapshots(pair)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(timestamp_utc)")
    conn.commit()
    conn.close()


def init_db():
    _ensure_db()


def save_raw_response(exchange: str, fiat: str, trade_type: str, raw):
    """Guarda la respuesta cruda (lista/dict) como JSON en la DB."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    payload = json.dumps(raw, ensure_ascii=False)
    timestamp = datetime.now(timezone.utc).isoformat()
    cur.execute(
        "INSERT INTO raw_responses (timestamp_utc, exchange, fiat, trade_type, raw_json) VALUES (?,?,?,?,?)",
        (timestamp, exchange, fiat, trade_type, payload),
    )
    conn.commit()
    conn.close()


def save_snapshot_summary(pair: str, summary: dict):
    """Guarda un snapshot resumido en la tabla `snapshots`.

    `summary` se espera que contenga claves compatibles con el antiguo CSV.
    """
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    ts = summary.get("timestamp_utc") or datetime.now(timezone.utc).isoformat()
    raw = json.dumps(summary, ensure_ascii=False)
    cur.execute(
        """
        INSERT INTO snapshots (
            timestamp_utc, pair, rows_fetched, avg_price_simple,
            avg_price_weighted, spread_pct, coef_var, total_exposed_volume,
            top1_price, top1_vol, top1_nick, top3_prices,
            arb_estimate_cop_to_ves_pct, arb_estimate_ves_to_cop_pct, raw_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            ts,
            summary.get("pair"),
            summary.get("rows_fetched"),
            summary.get("avg_price_simple"),
            summary.get("avg_price_weighted"),
            summary.get("spread_pct"),
            summary.get("coef_var"),
            summary.get("total_exposed_volume"),
            summary.get("top1_price"),
            summary.get("top1_vol"),
            summary.get("top1_nick"),
            summary.get("top3_prices"),
            summary.get("arb_estimate_cop_to_ves_pct"),
            summary.get("arb_estimate_ves_to_cop_pct"),
            raw,
        ),
    )
    conn.commit()
    conn.close()


def fetch_latest_snapshots(limit: int = 10):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT timestamp_utc, pair, raw_json FROM snapshots ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    results = []
    for ts, pair, raw in rows:
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        results.append({"timestamp_utc": ts, "pair": pair, "raw": parsed})
    return results


def get_latest_snapshot_for_pair(pair: str):
    """Devuelve la última snapshot almacenada para `pair` (por ejemplo 'USDT-COP')."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT timestamp_utc, raw_json FROM snapshots WHERE pair = ? ORDER BY id DESC LIMIT 1", (pair,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    ts, raw = row
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = raw
    return {"timestamp_utc": ts, "pair": pair, "raw": parsed}


def query_snapshots(pair: str = None, since: str = None, limit: int = 100):
    """Consulta snapshots con filtros simples.

    - `pair`: filtra por par (ej. 'USDT-COP')
    - `since`: fecha ISO (incluye desde esa fecha)
    - `limit`: número máximo de resultados
    """
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    q = "SELECT timestamp_utc, pair, raw_json FROM snapshots"
    conds = []
    params = []
    if pair:
        conds.append("pair = ?")
        params.append(pair)
    if since:
        conds.append("timestamp_utc >= ?")
        params.append(since)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    out = []
    for ts, p, raw in rows:
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        out.append({"timestamp_utc": ts, "pair": p, "raw": parsed})
    return out


def fetch_latest_raw(exchange: str = None, fiat: str = None, trade_type: str = None, limit: int = 10):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    q = "SELECT timestamp_utc, exchange, fiat, trade_type, raw_json FROM raw_responses"
    conds = []
    params = []
    if exchange:
        conds.append("exchange = ?")
        params.append(exchange)
    if fiat:
        conds.append("fiat = ?")
        params.append(fiat)
    if trade_type:
        conds.append("trade_type = ?")
        params.append(trade_type)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    results = []
    for ts, exch, f, t, raw_json in rows:
        try:
            parsed = json.loads(raw_json)
        except Exception:
            parsed = raw_json
        results.append({"timestamp_utc": ts, "exchange": exch, "fiat": f, "trade_type": t, "raw": parsed})
    return results
