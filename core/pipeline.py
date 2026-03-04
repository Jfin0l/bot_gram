import math
import statistics
from core import db
from datetime import datetime, timezone


def _extract_prices_and_vols(raw_list):
    precios = []
    volums = []
    if not raw_list:
        return precios, volums
    try:
        for item in raw_list:
            # item expected to be a Binance 'adv' dict inside the data list
            adv = item.get("adv", {}) if isinstance(item, dict) else {}
            precio = float(adv.get("price", 0) or 0)
            volumen = float(adv.get("dynamicMaxSingleTransAmount", 0) or 0)
            precios.append(precio)
            volums.append(volumen)
    except Exception:
        pass
    return precios, volums


def _analyze_list(precios, volums, config):
    if not precios:
        return {
            "avg": None,
            "avg_ponderado": None,
            "desv_std": None,
            "coef_var": None,
            "outliers": None,
            "min": None,
            "max": None,
            "raw_count": 0,
        }

    n = len(precios)
    avg_simple = sum(precios) / n
    if config.get("ponderacion_volumen", False) and sum(volums) > 0:
        avg_ponderado = sum(
            p * v for p, v in zip(precios, volums)) / sum(volums)
    else:
        avg_ponderado = avg_simple

    # stdev
    if n > 1:
        mean = avg_simple
        var = sum((p - mean) ** 2 for p in precios) / (n - 1)
        desv_std = math.sqrt(var)
    else:
        desv_std = 0

    coef_var = (desv_std / avg_simple) * \
        100 if avg_simple and avg_simple != 0 else 0

    limite_sup = avg_simple * (1 + config.get("limite_outlier", 0.025))
    limite_inf = avg_simple * (1 - config.get("limite_outlier", 0.025))
    precios_filtrados = [p for p in precios if limite_inf <= p <= limite_sup]
    outliers = len(precios) - len(precios_filtrados)

    return {
        "avg": round(avg_simple, 6),
        "avg_ponderado": round(avg_ponderado, 6),
        "desv_std": round(desv_std, 6),
        "coef_var": round(coef_var, 4),
        "outliers": outliers,
        "min": min(precios),
        "max": max(precios),
        "raw_count": n,
    }


def _build_data_structure(cop_buy, cop_sell, ves_buy, ves_sell, config):
    # Para tasas usamos una ventana profunda (posiciones 40-60)
    start, end = 40, 60

    def extract_prices_deep(lista):
        try:
            # Si es lista de Snapshots (Ads), convertirlas a formato dummy compatible o extraer directo
            # Si es lista de ads de RAM:
            if lista and hasattr(lista[0], 'price'):
                sub_list = lista[start:end] or lista[10:30]
                return [float(x.price) for x in sub_list]

            # Si es lista de dicts (fetch_latest_raw):
            sub_list = lista[start:end] or lista[10:30]
            prices = [float(x.get("adv", {}).get("price"))
                      for x in sub_list if (x and x.get("adv"))]
            return prices
        except Exception:
            return []

    precios_cop_buy = extract_prices_deep(cop_buy)
    precios_cop_sell = extract_prices_deep(cop_sell)
    precios_ves_buy = extract_prices_deep(ves_buy)
    precios_ves_sell = extract_prices_deep(ves_sell)

    def get_stable_avg(prices):
        if not prices:
            return None
        return statistics.median(prices)

    avg_cop_buy = get_stable_avg(precios_cop_buy)
    avg_cop_sell = get_stable_avg(precios_cop_sell)
    avg_ves_buy = get_stable_avg(precios_ves_buy)
    avg_ves_sell = get_stable_avg(precios_ves_sell)

    tasa_cop_ves_5 = (avg_cop_buy / avg_ves_sell *
                      1.05) if avg_cop_buy and avg_ves_sell else None
    tasa_cop_ves_10 = (avg_cop_buy / avg_ves_sell *
                       1.10) if avg_cop_buy and avg_ves_sell else None
    tasa_ves_cop_5 = (avg_ves_buy / avg_cop_sell *
                      1.05) if avg_ves_buy and avg_cop_sell else None

    # análisis profundo (usando helpers que ya aceptan dicts)
    # Si son Ads de RAM, convertirlos a dicts para compatibilidad temporal o actualizar helpers
    def to_raw(lista):
        if not lista:
            return []
        if hasattr(lista[0], 'price'):
            return [{"adv": {"price": x.price, "dynamicMaxSingleTransAmount": x.quantity}} for x in lista]
        return lista

    precios_cb, vol_cb = _extract_prices_and_vols(to_raw(cop_buy))
    precios_cs, vol_cs = _extract_prices_and_vols(to_raw(cop_sell))
    precios_vb, vol_vb = _extract_prices_and_vols(to_raw(ves_buy))
    precios_vs, vol_vs = _extract_prices_and_vols(to_raw(ves_sell))

    info_cop_buy = _analyze_list(precios_cb, vol_cb, config)
    info_cop_sell = _analyze_list(precios_cs, vol_cs, config)
    info_ves_buy = _analyze_list(precios_vb, vol_vb, config)
    info_ves_sell = _analyze_list(precios_vs, vol_vs, config)

    def _choose_avg(info):
        return info.get("avg_ponderado") or info.get("avg")

    avg_cop_buy_for_arbit = _choose_avg(info_cop_buy)
    avg_cop_sell_for_arbit = _choose_avg(info_cop_sell)
    avg_ves_buy_for_arbit = _choose_avg(info_ves_buy)
    avg_ves_sell_for_arbit = _choose_avg(info_ves_sell)

    # Arbitraje Cross-Border (COP <-> VES)
    # Tasa implícita TAKER/MAKER: Compro USDT con COP, Vendo USDT por VES
    # Incluimos comisión de Binance (0.16% por cada operación)
    fee = 0.0016
    tasa_p2p_cop_ves = None
    if avg_cop_buy and avg_ves_sell:
        # Costo unitario aumentado por fee / Recibo unitario disminuido por fee
        tasa_p2p_cop_ves = (avg_cop_buy * (1 + fee)) / \
            (avg_ves_sell * (1 - fee))

    # Eficiencia comparada con nuestra tasa preferencial (+5%)
    eficiencia_cop_ves = None
    if tasa_p2p_cop_ves and tasa_cop_ves_5:
        # Si P2P es 6.50 y nuestra tasa es 6.80, P2P es mas eficiente para el que envia (-4.4%)
        eficiencia_cop_ves = (tasa_p2p_cop_ves / tasa_cop_ves_5 - 1) * 100

    return {
        "timestamps": {"utc": datetime.now(timezone.utc).isoformat()},
        "COP": {
            "promedio_buy_tasa": avg_cop_buy,
            "promedio_sell_tasa": avg_cop_sell,
            "raw_count": len(cop_buy) + len(cop_sell),
        },
        "VES": {
            "promedio_buy_tasa": avg_ves_buy,
            "promedio_sell_tasa": avg_ves_sell,
            "raw_count": len(ves_buy) + len(ves_sell),
        },
        "tasas_remesas": {
            "cop_ves_5pct": tasa_cop_ves_5,
            "cop_ves_10pct": tasa_cop_ves_10,
            "ves_cop_5pct": tasa_ves_cop_5,
        },
        "analisis": {
            "cop_buy": info_cop_buy,
            "cop_sell": info_cop_sell,
            "ves_buy": info_ves_buy,
            "ves_sell": info_ves_sell,
        },
        "arbitraje": {
            "tasa_p2p": tasa_p2p_cop_ves,
            "eficiencia_pct": eficiencia_cop_ves,
            "orientacion": "P2P más barato" if eficiencia_cop_ves and eficiencia_cop_ves < 0 else "Remesa más barata"
        },
        "raw": {
            "cop_buy_raw": cop_buy,
            "cop_sell_raw": cop_sell,
            "ves_buy_raw": ves_buy,
            "ves_sell_raw": ves_sell,
        },
    }


def build_data_from_db(config: dict):
    """Construye el mismo diccionario de salida que antes, pero leyendo la última entrada guardada."""
    fiats = list(config.get("monedas", {}).keys()) or ["COP", "VES"]
    fetched = {}
    for fiat in fiats:
        for tt in ("BUY", "SELL"):
            rows = db.fetch_latest_raw(
                exchange=None, fiat=fiat, trade_type=tt, limit=1)
            fetched[f"{fiat}_{tt}"] = rows[0]["raw"] if rows else []

    return _build_data_structure(
        fetched.get("COP_BUY", []), fetched.get("COP_SELL", []),
        fetched.get("VES_BUY", []), fetched.get("VES_SELL", []),
        config
    )


def build_data_from_ram(config: dict):
    from core.ram_window import get_global
    rw = get_global()
    if not rw:
        return None

    with rw.lock:
        def get_ads(pair, side):
            dq = rw.pair_index.get(pair)
            if not dq:
                return []
            snap = dq[-1]
            return [a for a in snap.ads if a.side == side]

        cb = get_ads('USDT-COP', 'buy')
        cs = get_ads('USDT-COP', 'sell')
        vb = get_ads('USDT-VES', 'buy')
        vs = get_ads('USDT-VES', 'sell')

        if not cb and not vb:
            return None
        return _build_data_structure(cb, cs, vb, vs, config)
