import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
import logging

DB_PATH = Path("data/p2p_data.db")
logger = logging.getLogger(__name__)


def init_user_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_usage_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            command TEXT NOT NULL,
            result TEXT NOT NULL,
            response_time REAL,
            details TEXT
        )
        """
    )
    # Índices para búsquedas rápidas diarias
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_user_ts ON bot_usage_logs(user_id, timestamp)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            user_id TEXT PRIMARY KEY,
            reason TEXT,
            timestamp TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_waitlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            date TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            status TEXT DEFAULT 'WAITING',
            UNIQUE(user_id, date)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id TEXT PRIMARY KEY,
            currency TEXT DEFAULT 'COP'
        )
        """
    )

    conn.commit()
    conn.close()


def log_usage(user_id: str, command: str, result: str, response_time: float = 0.0, details: dict = None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    ts = datetime.now(timezone.utc).isoformat()
    det_json = json.dumps(details, ensure_ascii=False) if details else None
    try:
        cur.execute(
            "INSERT INTO bot_usage_logs (user_id, timestamp, command, result, response_time, details) VALUES (?,?,?,?,?,?)",
            (str(user_id), ts, command, result, response_time, det_json)
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Error guardando log de uso: {e}")
    finally:
        conn.close()


def is_blacklisted(user_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM blacklist WHERE user_id = ?",
                    (str(user_id),))
        return cur.fetchone() is not None
    finally:
        conn.close()


def set_blacklist_status(user_id: str, ban: bool, reason: str = ""):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        if ban:
            ts = datetime.now(timezone.utc).isoformat()
            cur.execute("INSERT OR REPLACE INTO blacklist (user_id, reason, timestamp) VALUES (?,?,?)", (str(
                user_id), reason, ts))
        else:
            cur.execute("DELETE FROM blacklist WHERE user_id = ?",
                        (str(user_id),))
        conn.commit()
    finally:
        conn.close()


def add_to_waitlist(user_id: str) -> int:
    """Añade a la waitlist de hoy si no está. Retorna la posición."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    today = datetime.now(timezone.utc).isoformat()[:10]
    ts = datetime.now(timezone.utc).isoformat()
    try:
        # Avoid overriding status if already there, ignore or do nothing
        cur.execute("INSERT OR IGNORE INTO daily_waitlist (user_id, date, timestamp, status) VALUES (?,?,?,?)",
                    (str(user_id), today, ts, 'WAITING'))
        conn.commit()

        # Calculate position
        cur.execute("SELECT COUNT(*) FROM daily_waitlist WHERE date = ? AND status = 'WAITING' AND timestamp <= (SELECT timestamp FROM daily_waitlist WHERE user_id = ? AND date = ? LIMIT 1)",
                    (today, str(user_id), today))
        pos = cur.fetchone()[0]
        return pos
    except Exception as e:
        logger.error(f"Error add_to_waitlist: {e}")
        return -1
    finally:
        conn.close()


def get_next_in_waitlist() -> str:
    """Obtiene el user_id del siguiente en waitlist o None. Lo marca como PROMOTED."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    today = datetime.now(timezone.utc).isoformat()[:10]
    try:
        cur.execute(
            "SELECT user_id FROM daily_waitlist WHERE date = ? AND status = 'WAITING' ORDER BY timestamp ASC LIMIT 1", (today,))
        row = cur.fetchone()
        if row:
            uid = row[0]
            cur.execute(
                "UPDATE daily_waitlist SET status = 'PROMOTED' WHERE user_id = ? AND date = ?", (uid, today))
            conn.commit()
            return uid
        return None
    except Exception as e:
        logger.error(f"Error get_next_in_waitlist: {e}")
        return None
    finally:
        conn.close()


def check_daily_limits(user_id: str, max_users: int = 30, max_requests_per_user: int = 15) -> tuple[bool, str]:
    """
    Verifica si el user_id puede ejecutar un comando hoy.
    """
    if is_blacklisted(user_id):
        return False, "BANNED"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    today_prefix = datetime.now(timezone.utc).isoformat()[:10]

    try:
        cur.execute(
            "SELECT COUNT(*) FROM bot_usage_logs WHERE user_id = ? AND timestamp LIKE ? AND result NOT IN ('LIMIT_USER', 'CAPACITY_FULL', 'WAITLIST', 'BANNED', 'ALREADY_WAITLIST')",
            (str(user_id), f"{today_prefix}%")
        )
        user_reqs = cur.fetchone()[0]

        # 1. Ya alcanzó sus consultas máximas
        if user_reqs >= max_requests_per_user:
            return False, "USER_LIMIT_REACHED"

        # 2. Tiene solicitudes válidas parciales >0 (Ocupa un Slot)
        if user_reqs > 0:
            return True, "OK"

        # 3. No tiene solicitudes. Revisar si fue promovido recientemente en waitlist.
        cur.execute("SELECT 1 FROM daily_waitlist WHERE user_id = ? AND date = ? AND status = 'PROMOTED'", (str(
            user_id), today_prefix))
        was_promoted = cur.fetchone() is not None
        if was_promoted:
            return True, "OK"

        # 4. Chequeo de Slots Activos Totales
        # Usuarios que están logueados SIN llegar al límite (ocupan slots)
        cur.execute('''
            SELECT user_id, COUNT(*) as reqs
            FROM bot_usage_logs
            WHERE timestamp LIKE ? AND result NOT IN ('LIMIT_USER', 'CAPACITY_FULL', 'WAITLIST', 'BANNED', 'ALREADY_WAITLIST')
            GROUP BY user_id
        ''', (f"{today_prefix}%",))
        rows = cur.fetchall()

        users_in_logs = set(row[0] for row in rows)
        active_from_logs = sum(
            1 for row in rows if row[1] < max_requests_per_user)

        # Usuarios promovidos que aún no tiran su primer query valido
        cur.execute(
            "SELECT user_id FROM daily_waitlist WHERE date = ? AND status = 'PROMOTED'", (today_prefix,))
        promoted_users = set(row[0] for row in cur.fetchall())
        active_promoted_no_logs = len(promoted_users - users_in_logs)

        currently_active_slots = active_from_logs + active_promoted_no_logs

        if currently_active_slots >= max_users:
            cur.execute("SELECT status FROM daily_waitlist WHERE user_id = ? AND date = ?", (str(
                user_id), today_prefix))
            wl_status = cur.fetchone()
            if wl_status and wl_status[0] == 'WAITING':
                cur.execute("SELECT COUNT(*) FROM daily_waitlist WHERE date = ? AND status = 'WAITING' AND timestamp <= (SELECT timestamp FROM daily_waitlist WHERE user_id = ? AND date = ? LIMIT 1)",
                            (today_prefix, str(user_id), today_prefix))
                pos = cur.fetchone()[0]
                return False, f"ALREADY_WAITLIST_{pos}"

            pos = add_to_waitlist(user_id)
            return False, f"WAITLIST_{pos}"

        return True, "OK"
    except Exception as e:
        logger.error(f"Error verificando límites: {e}")
        return True, "ERROR_ALLOW"
    finally:
        conn.close()


def get_user_currency(user_id: str) -> str:
    """Retorna la moneda preferida del usuario, default 'COP'."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT currency FROM user_preferences WHERE user_id = ?", (str(user_id),))
        row = cur.fetchone()
        return row[0] if row else "COP"
    finally:
        conn.close()


def set_user_currency(user_id: str, currency: str):
    """Establece la moneda preferida del usuario."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR REPLACE INTO user_preferences (user_id, currency) VALUES (?, ?)", (str(user_id), currency.upper()))
        conn.commit()
    finally:
        conn.close()
