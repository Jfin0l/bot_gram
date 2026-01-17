# core/storage.py
import csv, json
from datetime import datetime, UTC
from pathlib import Path
from config import log

def _ensure_folder(path: str):
    """Crea el directorio si no existe."""
    Path(path).mkdir(parents=True, exist_ok=True)

def save_ads_raw(pair: str, tradeType: str, ads: list, exchange="binance_p2p"):
    """
    Guarda los anuncios crudos (una fila por anuncio) en data/ads/YYYYMMDD.csv
    """
    date = datetime.now(UTC).strftime("%Y%m%d")
    folder = Path("data/ads")
    _ensure_folder(folder)
    filename = folder / f"ads_{date}.csv"
    fieldnames = [
        "timestamp_utc", "exchange", "pair", "tradeType", "adv_id",
        "advertiser_nick", "monthFinishRate", "monthOrderCount",
        "price", "dynamicMaxSingleTransAmount", "maxSingleTransAmount",
        "minSingleTransAmount", "payment_methods", "raw_json"
    ]

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if f.tell() == 0:
            writer.writeheader()

        for ad in ads:
            try:
                writer.writerow({
                    "timestamp_utc": datetime.now(UTC).isoformat(),
                    "exchange": exchange,
                    "pair": pair,
                    "tradeType": tradeType,
                    "adv_id": ad.get("adv", {}).get("advNo"),
                    "advertiser_nick": ad.get("advertiser", {}).get("nickName"),
                    "monthFinishRate": ad.get("advertiser", {}).get("monthFinishRate"),
                    "monthOrderCount": ad.get("advertiser", {}).get("monthOrderCount"),
                    "price": ad.get("adv", {}).get("price"),
                    "dynamicMaxSingleTransAmount": ad.get("adv", {}).get("dynamicMaxSingleTransAmount"),
                    "maxSingleTransAmount": ad.get("adv", {}).get("maxSingleTransAmount"),
                    "minSingleTransAmount": ad.get("adv", {}).get("minSingleTransAmount"),
                    "payment_methods": ",".join([m.get("tradeMethodName", "") for m in ad.get("adv", {}).get("tradeMethods", [])]),
                    "raw_json": json.dumps(ad)[:500]  # truncamos por seguridad
                })
            except Exception as e:
                log.warning(f"⚠️ Error guardando anuncio: {e}")

    log.info(f"💾 Datos RAW guardados ({len(ads)} anuncios {tradeType}) → {filename}")

def save_snapshot_summary(pair: str, summary: dict):
    """
    Guarda una fila por snapshot con datos agregados en data/snapshots/YYYYMMDD.csv
    """
    date = datetime.now(UTC).strftime("%Y%m%d")
    folder = Path("data/snapshots")
    _ensure_folder(folder)
    filename = folder / f"snapshots_{date}.csv"

    fieldnames = [
        "timestamp_utc", "pair", "rows_fetched", "avg_price_simple",
        "avg_price_weighted", "spread_pct", "coef_var",
        "total_exposed_volume", "top1_price", "top1_vol", "top1_nick",
        "top3_prices", "arb_estimate_cop_to_ves_pct",
        "arb_estimate_ves_to_cop_pct"
    ]

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(summary)

    log.info(f"💾 Snapshot guardado → {filename}")