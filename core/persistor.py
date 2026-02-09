# core/persistor.py
import os
from datetime import datetime
from config import SNAPSHOT_PATH, ADS_PATH, log
from core import db

def save_snapshot(pair, data):
    """Guarda snapshot usando la DB en lugar de CSV (migración)."""
    try:
        db.save_snapshot_summary(pair, data)
        log.info(f"Snapshot guardado en DB → {pair}")
    except Exception as e:
        log.warning(f"Error guardando snapshot en DB: {e}")

def save_ads(pair, ads):
    """Guarda anuncios crudos en la DB (migración desde CSV)."""
    if not ads:
        return

    try:
        # asumimos pair como 'USDT-COP' y tradeType no disponible aquí; lo guardamos como RAW
        fiat = pair.split("-")[1] if "-" in pair else pair
        db.save_raw_response("legacy", fiat, "RAW", ads)
        log.info(f"{len(ads)} anuncios guardados en DB para {pair}")
    except Exception as e:
        log.warning(f"Error guardando ads en DB: {e}")
