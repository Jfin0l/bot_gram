# main.py
from adapters.binance_p2p import get_ads
from core.storage import save_ads_raw, save_snapshot_summary
from core.analyzer import analyze_ads
from config import log
from datetime import datetime, UTC

def collect_snapshot(pair="USDT-COP"):
    log.info(f"📊 Iniciando snapshot de {pair}")
    fiat = pair.split("-")[1]

    buy_ads, sell_ads = get_ads(asset="USDT", fiat=fiat)

    if not buy_ads or not sell_ads:
        log.warning(f"⚠️ No se pudieron obtener anuncios para {pair}")
        return

    # Guardar crudos
    save_ads_raw(pair, "BUY", buy_ads)
    save_ads_raw(pair, "SELL", sell_ads)

    # Analizar resumen
    summary = analyze_ads(pair, buy_ads, sell_ads)
    if summary:
        save_snapshot_summary(pair, summary)
        log.info(f"✅ Snapshot completado: {summary}")
    else:
        log.warning(f"No se pudo analizar snapshot para {pair}")
