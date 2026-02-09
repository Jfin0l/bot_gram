# core/storage.py
import json
from datetime import datetime, UTC
from pathlib import Path
from config import log
from core import db

def _ensure_folder(path: str):
    """Crea el directorio si no existe."""
    Path(path).mkdir(parents=True, exist_ok=True)

def save_ads_raw(pair: str, tradeType: str, ads: list, exchange="binance_p2p"):
    """Guarda anuncios crudos en la DB (migración desde CSV).

    Guarda la lista completa `ads` como una única entrada en la tabla
    `raw_responses` usando `core.db.save_raw_response`.
    """
    try:
        # extraer fiat desde el pair (ej. 'USDT-COP')
        fiat = pair.split("-")[1] if "-" in pair else pair
        db.save_raw_response(exchange, fiat, tradeType, ads)
        log.info(f"💾 Datos RAW guardados en DB ({len(ads)} anuncios {tradeType}) pair={pair}")
    except Exception as e:
        log.warning(f"⚠️ Error guardando anuncios en DB: {e}")

def save_snapshot_summary(pair: str, summary: dict):
    """Guarda snapshot usando la DB central (migración de CSV a SQLite)."""
    try:
        db.save_snapshot_summary(pair, summary)
        log.info(f"💾 Snapshot guardado en DB (pair={pair})")
    except Exception as e:
        log.warning(f"⚠️ Error guardando snapshot en DB: {e}")