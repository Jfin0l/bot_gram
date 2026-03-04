from core import pipeline, db
from config import log


def create_and_store_snapshots(config):
    """Genera snapshots a partir del pipeline y los guarda en la DB.

    Crea una entrada por par (USDT-COP y USDT-VES) con el formato esperado.
    """
    data = pipeline.build_data_from_db(config)
    if not data:
        log.warning("No hay datos para crear snapshot")
        return

    # para cada par guardamos un resumen
    # COP
    try:
        cop_summary = {
            "timestamp_utc": data["timestamps"]["utc"],
            "pair": "USDT-COP",
            "rows_fetched": data["COP"]["raw_count"],
            "avg_price_simple": data["COP"]["promedio_buy_tasa"],
            "avg_price_weighted": data["analisis"]["cop_buy"]["avg_ponderado"],
            "spread_pct": data["analisis"]["cop_buy"]["coef_var"],
            "coef_var": data["analisis"]["cop_buy"]["coef_var"],
            "total_exposed_volume": None,
            "top1_price": data["analisis"]["cop_buy"].get("min"),
            "top1_vol": None,
            "top1_nick": None,
            "top3_prices": str([]),
            "arb_estimate_cop_to_ves_pct": data["arbitraje"].get("eficiencia_pct") if data.get("arbitraje") else None,
            "arb_estimate_ves_to_cop_pct": data["arbitraje"].get("tasa_p2p") if data.get("arbitraje") else None,
        }
        db.save_snapshot_summary("USDT-COP", cop_summary)
    except Exception as e:
        log.warning(f"Error guardando snapshot COP: {e}")

    # VES
    try:
        ves_summary = {
            "timestamp_utc": data["timestamps"]["utc"],
            "pair": "USDT-VES",
            "rows_fetched": data["VES"]["raw_count"],
            "avg_price_simple": data["VES"]["promedio_buy_tasa"],
            "avg_price_weighted": data["analisis"]["ves_buy"]["avg_ponderado"],
            "spread_pct": data["analisis"]["ves_buy"]["coef_var"],
            "coef_var": data["analisis"]["ves_buy"]["coef_var"],
            "total_exposed_volume": None,
            "top1_price": data["analisis"]["ves_buy"].get("min"),
            "top1_vol": None,
            "top1_nick": None,
            "top3_prices": str([]),
            "arb_estimate_cop_to_ves_pct": data["arbitraje"].get("eficiencia_pct") if data.get("arbitraje") else None,
            "arb_estimate_ves_to_cop_pct": data["arbitraje"].get("tasa_p2p") if data.get("arbitraje") else None,
        }
        db.save_snapshot_summary("USDT-VES", ves_summary)
    except Exception as e:
        log.warning(f"Error guardando snapshot VES: {e}")
