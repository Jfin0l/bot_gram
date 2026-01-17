# core/persistor.py
import csv
import os
from datetime import datetime
from config import SNAPSHOT_PATH, ADS_PATH, log

def save_snapshot(pair, data):
    """Guarda snapshot resumido por par"""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    file_path = os.path.join(SNAPSHOT_PATH, f"snapshots_{date_str}.csv")
    file_exists = os.path.exists(file_path)

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)
    log.info(f"Snapshot guardado → {pair}")

def save_ads(pair, ads):
    """Guarda anuncios crudos en archivo ads/"""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    file_path = os.path.join(ADS_PATH, f"{pair}_{date_str}_ads.csv")
    file_exists = os.path.exists(file_path)

    if not ads:
        return

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ads[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(ads)
    log.info(f"{len(ads)} anuncios guardados para {pair}")
