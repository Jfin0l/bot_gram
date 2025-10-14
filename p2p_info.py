""" Arbitraje final"""

import datetime
import json
import time
import pytz
import requests
import os
from dotenv import load_dotenv

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
def buy_copusdt():
    url = 'https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search'

    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    }

    dataB = {
        "page": 1,
        "rows": 10,
        "asset": "USDT",
        "tradeType": "BUY",
        "fiat": "COP",
        "merchantCheck": False
    }

    dataS = {
        "page": 1,
        "rows": 10,
        "asset": "USDT",
        "tradeType": "SELL",
        "fiat": "VES",
        "merchantCheck": False 
    }

    try:
        # Consulta compra USDT/COP
        r_buy = requests.post(url, headers=headers, json=dataB, timeout=10)
        r_buy.raise_for_status()
        datos_buy = r_buy.json().get('data', [])
        
        # Consulta venta USDT/VES
        r_sell = requests.post(url, headers=headers, json=dataS, timeout=10)
        r_sell.raise_for_status()
        datos_sell = r_sell.json().get('data', [])

        if len(datos_buy) < 5 or len(datos_sell) < 5:
            return "⚠️ Datos insuficientes en Binance P2P. Intenta más tarde."

        precio_cop = float(datos_buy[4]['adv']['price'])
        precio_ves = float(datos_sell[4]['adv']['price'])

        tasa1 = precio_cop / precio_ves * 1.05
        tasa2 = precio_cop / precio_ves * 1.075
        tasa3 = precio_cop / precio_ves * 1.10

        msj = f"""💱 Cotización P2P Binance
🇨🇴 COP = {precio_cop}
🇻🇪 VES = {precio_ves}

Tasas aproximadas:
📈 5%  → {round(tasa1, 2)}
📈 7.5% → {round(tasa2, 2)}
📈 10% → {round(tasa3, 2)}
"""
        return msj

    except Exception as e:
        return f"❌ Error obteniendo datos: {e}"

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
# Programa principal
# ==============================
if __name__ == "__main__":
    intervalo_tiempo = 3600  # segundos
    horas_programadas = [6, 8, 10, 14, 18, 20]

    while True:
        hora_actual = datetime.datetime.now(pytz.utc)
        bogota = pytz.timezone('America/Bogota')
        bogota_time = hora_actual.astimezone(bogota)
        hora = bogota_time.hour

        print("⏰ Activo:", bogota_time)

        if hora in horas_programadas:
            msj = buy_copusdt()
            enviar_mensaje(msj)

        time.sleep(intervalo_tiempo)
