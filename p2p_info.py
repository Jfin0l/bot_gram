import datetime
import json
import time
import pytz
import requests
import os
import statistics
from dotenv import load_dotenv

# ==============================
# CONFIGURACIÓN GLOBAL (Centralizada)
# ==============================
CONFIG = {
    "pares": ["USDT-COP", "USDT-VES"],  # pares de monedas a analizar
    "monedas": {
        "COP": {"rows": 20, "page": 1},  # rango amplio para análisis de arbitraje
        "VES": {"rows": 20, "page": 1}
    },
    "horas_programadas": [6, 8, 10, 14, 18, 20, 21, 22, 23],  # ejecución automática
    "intervalo_tiempo": 3600,  # segundos
    "umbral_volatilidad": 3,   # >3% se considera alta volatilidad
    "limite_outlier": 0.03,    # ±3% para detección de outliers
    "ponderacion_volumen": True,  # activar promedio ponderado por volumen
    "filas_tasa_remesa": 5      # cuántos anuncios usar para el cálculo de tasa
}

# ==============================
# Cargar variables del entorno (.env)
# ==============================
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise ValueError("❌ Error: Faltan variables TELEGRAM_TOKEN o CHAT_ID en el archivo .env")

# ==============================
# Función principal para obtener precios P2P
# ==============================
def conect_p2p():
    url = 'https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search'
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    }

    def get_data(tradeType, fiat):
        """Obtiene anuncios del mercado P2P según el tipo (BUY/SELL) y moneda (COP/VES)."""
        params = CONFIG["monedas"].get(fiat, {"rows": 10, "page": 1})
        payload = {
            "page": params["page"],
            "rows": params["rows"],
            "asset": "USDT",
            "tradeType": tradeType,
            "fiat": fiat,
            "merchantCheck": False
        }
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json().get("data", [])
            return data
        except Exception as e:
            print(f"❌ Error al obtener datos {tradeType}-{fiat}: {e}")
            return []

    # ==============================
    # Descarga de datos del mercado
    # ==============================
    cop_buy = get_data("BUY", "COP")
    cop_sell = get_data("SELL", "COP")
    ves_buy = get_data("BUY", "VES")
    ves_sell = get_data("SELL", "VES")

    # Datos para tasa (solo primeros 5 anuncios)
    cop_buy_tasa = cop_buy[:CONFIG["filas_tasa_remesa"]]
    cop_sell_tasa = cop_sell[:CONFIG["filas_tasa_remesa"]]
    ves_buy_tasa = ves_buy[:CONFIG["filas_tasa_remesa"]]
    ves_sell_tasa = ves_sell[:CONFIG["filas_tasa_remesa"]]

    if not cop_buy_tasa or not ves_sell_tasa:
        return "⚠️ No se pudieron obtener datos suficientes del mercado."

    # ==============================
    # Cálculo de tasas (Remesas)
    # ==============================
    precios_cop_buy = [float(x["adv"]["price"]) for x in cop_buy_tasa]
    precios_cop_sell = [float(x["adv"]["price"]) for x in cop_sell_tasa]
    precios_ves_buy = [float(x["adv"]["price"]) for x in ves_buy_tasa]
    precios_ves_sell = [float(x["adv"]["price"]) for x in ves_sell_tasa]

    avg_cop_buy = sum(precios_cop_buy) / len(precios_cop_buy)
    avg_cop_sell = sum(precios_cop_sell) / len(precios_cop_sell)
    avg_ves_buy = sum(precios_ves_buy) / len(precios_ves_buy)
    avg_ves_sell = sum(precios_ves_sell) / len(precios_ves_sell)

    best_cop_buy = min(precios_cop_buy)
    best_ves_sell = max(precios_ves_sell)

    # Spread y ratios de arbitraje
    spread_cop = (avg_cop_buy / avg_cop_sell - 1) * 100
    spread_ves = (avg_ves_buy / avg_ves_sell - 1) * 100
    tasa_5 = avg_cop_buy / avg_ves_sell * 1.05
    tasa_10 = avg_cop_buy / avg_ves_sell * 1.10
    tasa_ves_a_cop = avg_ves_buy / avg_cop_sell * 1.06

    # ==============================
    # Análisis Profundo del Mercado (para arbitraje)
    # ==============================
    info_cop_buy = analizar_mercado(cop_buy, "COP", "BUY")
    info_cop_sell = analizar_mercado(cop_sell, "COP", "SELL")
    info_ves_buy = analizar_mercado(ves_buy, "VES", "BUY")
    info_ves_sell = analizar_mercado(ves_sell, "VES", "SELL")

    stability_cop = "⚠️ Alta volatilidad" if info_cop_buy["coef_var"] > CONFIG["umbral_volatilidad"] else "✅ Estable"
    stability_ves = "⚠️ Alta volatilidad" if info_ves_sell["coef_var"] > CONFIG["umbral_volatilidad"] else "✅ Estable"

    # ==============================
    # Mensaje de salida
    # ==============================
    msj = f"""
📊 *Análisis P2P Binance*  
USDT ↔ COP/VES

💵 Promedio compra (COP): {avg_cop_buy:,.2f}
💵 Promedio venta (COP): {avg_cop_sell:,.2f}
- Promedio ampliado (análisis): {info_cop_buy["avg"]:,.2f}
- Ponderado (volumen): {info_cop_buy["avg_ponderado"]:,.2f}
📈 Spread promedio: {spread_cop:.2f} %
Estabilidad: {stability_cop}

💰 Promedio compra (VES): {avg_ves_buy:,.2f}
💰 Promedio venta (VES): {avg_ves_sell:,.2f}
- Promedio ampliado (análisis): {info_ves_sell["avg"]:,.2f}
- Ponderado (volumen): {info_ves_sell["avg_ponderado"]:,.2f}
📈 Spread promedio: {spread_ves:.2f} %
Estabilidad: {stability_ves}

💱 Tasas de referencia:
  +5%  → {tasa_5:,.2f}
  +10% → {tasa_10:,.2f}
  VES a COP → {tasa_ves_a_cop:,.5f}

📊 Volatilidad:
  COP → {info_cop_buy["coef_var"]:.2f}%
  VES → {info_ves_sell["coef_var"]:.2f}%

⚙️ Anuncios analizados: {len(cop_buy)} / {len(ves_sell)}
"""
    return msj


# ==============================
# Función de Análisis de Mercado P2P
# ==============================
def analizar_mercado(datos, fiat, tipo):
    """Analiza el mercado P2P y calcula métricas estadísticas para arbitraje."""
    if not datos:
        return None

    precios, volúmenes = [], []
    for x in datos:
        adv = x["adv"]
        try:
            precio = float(adv["price"])
            volumen = float(adv.get("dynamicMaxSingleTransAmount", 0))
            precios.append(precio)
            volúmenes.append(volumen)
        except:
            continue

    if not precios:
        return None

    avg_simple = sum(precios) / len(precios)
    avg_ponderado = (
        sum(p * v for p, v in zip(precios, volúmenes)) / sum(volúmenes)
        if CONFIG["ponderacion_volumen"] and sum(volúmenes) > 0
        else avg_simple
    )

    # --- Volatilidad estadística ---
    desv_std = statistics.stdev(precios) if len(precios) > 1 else 0
    coef_var = (desv_std / avg_simple) * 100 if avg_simple > 0 else 0

    # --- Filtro de outliers dinámico ---
    limite_sup = avg_simple * (1 + CONFIG["limite_outlier"])
    limite_inf = avg_simple * (1 - CONFIG["limite_outlier"])
    precios_filtrados = [p for p in precios if limite_inf <= p <= limite_sup]
    outliers = len(precios) - len(precios_filtrados)

    return {
        "fiat": fiat,
        "tipo": tipo,
        "avg": round(avg_simple, 2),
        "avg_ponderado": round(avg_ponderado, 2),
        "desv_std": round(desv_std, 2),
        "coef_var": round(coef_var, 2),
        "outliers": outliers,
        "min": min(precios),
        "max": max(precios)
    }


# ==============================
# Enviar mensaje por Telegram
# ==============================
def enviar_mensaje(msj):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msj}
    try:
        r = requests.post(url, data=data, timeout=10)
        response = r.json()
        if response.get('ok'):
            print("✅ Mensaje enviado exitosamente")
        else:
            print("⚠️ Error al enviar mensaje:", response.get('description'))
    except Exception as e:
        print(f"❌ Error al conectar con Telegram: {e}")


# ==============================
# Programa principal (ciclo automático)
# ==============================
if __name__ == "__main__":
    while True:
        hora_actual = datetime.datetime.now(pytz.utc)
        bogota = pytz.timezone('America/Bogota')
        hora_col = hora_actual.astimezone(bogota).hour

        print(f"⏰ Activo: {hora_col}:00h")

        if hora_col in CONFIG["horas_programadas"]:
            msj = conect_p2p()
            enviar_mensaje(msj)

        time.sleep(CONFIG["intervalo_tiempo"])
