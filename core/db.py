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
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_pair ON snapshots(pair)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(timestamp_utc)")
    # aggregated prices table (10-minute buckets)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS aggregated_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            bucket_start TEXT NOT NULL,
            avg_price REAL,
            min_price REAL,
            max_price REAL,
            volume REAL,
            spread_pct REAL,
            volatility REAL,
            sample_count INTEGER
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_agg_pair_bucket ON aggregated_prices(pair, bucket_start)")
    # events table (simple signals/anomalies)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            pair TEXT,
            timestamp TEXT NOT NULL,
            severity INTEGER,
            details TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp)")

    # Tabla de métricas históricas (para /spread dia, /volume, /depth)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_metrics_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            value REAL NOT NULL,
            timestamp TEXT NOT NULL,
            details TEXT
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_metrics_name_ts ON market_metrics_history(metric_name, timestamp)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_metrics_pair ON market_metrics_history(pair)")

    # Tabla para historial de anuncios de comerciantes (Top 50)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS merchant_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            merchant_id TEXT NOT NULL,
            merchant_name TEXT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            position INTEGER,
            volume REAL,
            timestamp TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_merchant_id ON merchant_history(merchant_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_merchant_ts ON merchant_history(timestamp)")

    # Tabla para perfiles persistentes y scores de automatización
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS merchant_registry (
            merchant_id TEXT PRIMARY KEY,
            nickname TEXT,
            automation_score REAL DEFAULT 0,
            classification TEXT DEFAULT 'HUMANO',
            last_seen TEXT,
            details TEXT
        )
        """
    )

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


def init_merchant_stats_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS merchant_stats (
            merchant TEXT,
            pair TEXT,
            side TEXT,
            volume_usdt REAL,
            avg_price REAL,
            ad_count INTEGER,
            hour INTEGER,
            date TEXT,
            UNIQUE(merchant, pair, side, date, hour)
        )
    """)
    conn.commit()
    conn.close()


def fetch_latest_snapshots(limit: int = 10):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp_utc, pair, raw_json FROM snapshots ORDER BY id DESC LIMIT ?", (limit,))
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


def save_aggregated_price(pair: str, bucket_start: str, avg_price: float = None, min_price: float = None,
                          max_price: float = None, volume: float = None, spread_pct: float = None,
                          volatility: float = None, sample_count: int = None):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO aggregated_prices (pair, bucket_start, avg_price, min_price, max_price, volume, spread_pct, volatility, sample_count) VALUES (?,?,?,?,?,?,?,?,?)",
        (pair, bucket_start, avg_price, min_price, max_price,
         volume, spread_pct, volatility, sample_count),
    )
    conn.commit()
    conn.close()


def fetch_recent_aggregates(pair: str, limit: int = 50):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT bucket_start, avg_price, min_price, max_price, volume, spread_pct, volatility, sample_count FROM aggregated_prices WHERE pair = ? ORDER BY id DESC LIMIT ?", (pair, limit))
    rows = cur.fetchall()
    conn.close()
    out = []
    for row in rows:
        out.append({
            'bucket_start': row[0],
            'avg_price': row[1],
            'min_price': row[2],
            'max_price': row[3],
            'volume': row[4],
            'spread_pct': row[5],
            'volatility': row[6],
            'sample_count': row[7],
        })
    return out


def save_event(event_type: str, pair: str, timestamp: str, details: dict = None, severity: int = 1):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    payload = json.dumps(details or {}, ensure_ascii=False)
    cur.execute("INSERT INTO events (event_type, pair, timestamp, severity, details) VALUES (?,?,?,?,?)",
                (event_type, pair, timestamp, severity, payload))
    conn.commit()
    conn.close()


def recent_event_exists(event_type: str, pair: str, within_seconds: int = 300, match_details: dict = None) -> bool:
    """Return True if a recent event of same type/pair exists within `within_seconds`.

    If `match_details` is provided, the function will compare the stored event `details`
    JSON and require that all keys in `match_details` match exactly.
    """
    _ensure_db()
    import datetime

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # fetch recent events of this type/pair (limit to reasonable number)
    cur.execute("SELECT timestamp, details FROM events WHERE event_type = ? AND pair = ? ORDER BY id DESC LIMIT 200", (event_type, pair))
    rows = cur.fetchall()
    conn.close()

    now = datetime.datetime.now(datetime.timezone.utc)
    for ts_str, details_json in rows:
        try:
            ts = datetime.datetime.fromisoformat(ts_str)
        except Exception:
            # fallback: skip unparsable timestamps
            continue
        delta = (now - ts).total_seconds()
        if delta <= within_seconds:
            if match_details:
                try:
                    parsed = json.loads(details_json or "{}")
                except Exception:
                    parsed = {}
                ok = True
                for k, v in match_details.items():
                    if parsed.get(k) != v:
                        ok = False
                        break
                if ok:
                    return True
                else:
                    continue
            else:
                return True
    return False


def save_event_dedup(event_type: str, pair: str, timestamp: str, details: dict = None, severity: int = 1, dedup_seconds: int = 300, match_details: dict = None):
    """Save event only if a similar recent event does not exist.

    `match_details` is forwarded to `recent_event_exists` to compare specific fields (e.g., merchant).
    """
    if recent_event_exists(event_type, pair, within_seconds=dedup_seconds, match_details=match_details):
        return False
    save_event(event_type, pair, timestamp, details=details, severity=severity)
    return True


def get_latest_snapshot_for_pair(pair: str):
    """Devuelve la última snapshot almacenada para `pair` (por ejemplo 'USDT-COP')."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp_utc, raw_json FROM snapshots WHERE pair = ? ORDER BY id DESC LIMIT 1", (pair,))
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
        results.append({"timestamp_utc": ts, "exchange": exch,
                       "fiat": f, "trade_type": t, "raw": parsed})
    return results


def save_market_metric(pair: str, metric_name: str, value: float, details: dict = None):
    """Guarda una métrica de mercado histórica."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    ts = datetime.now(timezone.utc).isoformat()
    det_json = json.dumps(details, ensure_ascii=False) if details else None
    cur.execute(
        "INSERT INTO market_metrics_history (pair, metric_name, value, timestamp, details) VALUES (?,?,?,?,?)",
        (pair, metric_name, value, ts, det_json)
    )
    conn.commit()
    conn.close()


def fetch_metrics_history(pair: str, metric_name: str, since_hours: int = 24):
    """Obtiene el historial de una métrica específica."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cutoff = datetime.now(timezone.utc).replace(microsecond=0)
    import datetime as dt_mod
    cutoff = (cutoff - dt_mod.timedelta(hours=since_hours)).isoformat()

    cur.execute(
        "SELECT timestamp, value, details FROM market_metrics_history WHERE pair = ? AND metric_name = ? AND timestamp >= ? ORDER BY timestamp ASC",
        (pair, metric_name, cutoff)
    )
    rows = cur.fetchall()
    conn.close()
    return [{"timestamp": r[0], "value": r[1], "details": json.loads(r[2]) if r[2] else None} for r in rows]
