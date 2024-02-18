""" Arbitraje final"""

import datetime
import json
import time

import pytz
import requests

def buy_copusdt():
    url = 'https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search'

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "123",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
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

    r = requests.post(url, headers=headers, json=dataB)
    r_json = r.json()
    datos_buy = r_json['data']

    r = requests.post(url, headers=headers, json=dataS)
    r_json = r.json()
    datos_sell = r_json['data']

    precio_cop = datos_buy[4]['adv']['price']
    precio_ves = datos_sell[0]['adv']['price']
    tasa1 = float(precio_cop) / float(precio_ves) * 1.05
    tasa2 = float(precio_cop) / float(precio_ves) * 1.075
    tasa3 = float(precio_cop) / float(precio_ves) * 1.10

    msj = f"""COP = {precio_cop}, VES = {precio_ves},
{round(tasa1, 2)} 5%
{round(tasa2, 2)} 7,5%
{round(tasa3, 2)} 10%
         """

    return msj


def generar_mensaje():
    return buy_copusdt()

# Definir el intervalo de tiempo en segundos entre cada mensaje
intervalo_tiempo = 3600 #* 60 * 60  # 8 horas en segundos
horas_programadas = [6, 10, 14, 18]

while True:
    hora_actual = datetime.datetime.now(pytz.utc)
    bogota = pytz.timezone('America/Bogota')
    bogota_time = hora_actual.astimezone(bogota)
    print('activo', bogota_time)

    if bogota_time.hour in horas_programadas:

        msj = buy_copusdt()
        data = {'chat_id':'-4068874959', 'text':msj}
        url = 'https://api.telegram.org/bot6386867007:AAHgElLPR99d00OzFxvcRrIFgBl0OeTDG_g/sendMessage'
        r = requests.post(url, data)
        data = json.loads(r.text)

        response_data = json.loads(r.text)

# Verifica si el mensaje se envió correctamente
        if response_data['ok']:
            print('Mensaje enviado exitosamente', bogota_time)
        else:
            print('Error al enviar el mensaje:', response_data['description'], bogota_time)

    # Esperar el intervalo de tiempo antes de generar el siguiente mensaje

    time.sleep(intervalo_tiempo)
    