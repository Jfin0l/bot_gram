# p2p_info.py
import os
import requests
import statistics
import asyncio
import pytz
import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# ==============================
# CONFIGURACIÓN GLOBAL (Centralizada)
# ==============================
CONFIG = {
    "pares": ["USDT-COP", "USDT-VES"],
    "monedas": {
        "COP": {"rows": 20, "page": 2},  # usas página 2 por tu preferencia
        "VES": {"rows": 20, "page": 4}
    },
    "horas_programadas": [6, 8, 10, 14, 17, 18, 20, 21, 22, 23],
    "intervalo_tiempo": 3600,
    "umbral_volatilidad": 3,
    "limite_outlier": 0.025,
    "ponderacion_volumen": True,
    "filas_tasa_remesa": 5
}

# ==============================
# Cargar variables del entorno (.env)
# ==============================
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
if not TELEGRAM_TOKEN or not CHAT_ID:
    raise ValueError("Faltan TELEGRAM_TOKEN o CHAT_ID en .env")

# Scheduler global
scheduler = AsyncIOScheduler(timezone=pytz.timezone("America/Bogota"))
auto_job = None

# ==============================
# FUNCIONES DE DATOS (sin formateo)
# ==============================

def _fetch_ads(tradeType: str, fiat: str):
    """Hace la llamada POST a Binance P2P y devuelve la lista 'data' (sin procesar)."""
    url = 'https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search'
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
    }
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
        return r.json().get("data", [])
    except Exception as e:
        print(f"❌ Error al obtener datos {tradeType}-{fiat}: {e}")
        return []

def analizar_mercado(datos, fiat, tipo):
    """Analiza el mercado P2P y calcula métricas estadísticas para arbitraje."""
    if not datos:
        return {
            "fiat": fiat, "tipo": tipo,
            "avg": None, "avg_ponderado": None,
            "desv_std": None, "coef_var": None,
            "outliers": None, "min": None, "max": None,
            "raw_count": 0
        }

    precios, volúmenes = [], []
    for x in datos:
        adv = x.get("adv", {})
        try:
            precio = float(adv.get("price", 0))
            volumen = float(adv.get("dynamicMaxSingleTransAmount", 0) or 0)
            precios.append(precio)
            volúmenes.append(volumen)
        except Exception:
            continue

    if not precios:
        return {
            "fiat": fiat, "tipo": tipo,
            "avg": None, "avg_ponderado": None,
            "desv_std": None, "coef_var": None,
            "outliers": None, "min": None, "max": None,
            "raw_count": 0
        }

    avg_simple = sum(precios) / len(precios)
    if CONFIG["ponderacion_volumen"] and sum(volúmenes) > 0:
        avg_ponderado = sum(p * v for p, v in zip(precios, volúmenes)) / sum(volúmenes)
    else:
        avg_ponderado = avg_simple

    desv_std = statistics.stdev(precios) if len(precios) > 1 else 0
    coef_var = (desv_std / avg_simple) * 100 if avg_simple > 0 else 0

    limite_sup = avg_simple * (1 + CONFIG["limite_outlier"])
    limite_inf = avg_simple * (1 - CONFIG["limite_outlier"])
    precios_filtrados = [p for p in precios if limite_inf <= p <= limite_sup]
    outliers = len(precios) - len(precios_filtrados)

    return {
        "fiat": fiat,
        "tipo": tipo,
        "avg": round(avg_simple, 6) if avg_simple is not None else None,
        "avg_ponderado": round(avg_ponderado, 6) if avg_ponderado is not None else None,
        "desv_std": round(desv_std, 6),
        "coef_var": round(coef_var, 4),
        "outliers": outliers,
        "min": min(precios),
        "max": max(precios),
        "raw_count": len(precios)
    }

def get_p2p_data():
    """
    Obtiene datos P2P y retorna un diccionario estructurado con:
    - promedios para tasas (primeros N anuncios)
    - análisis ampliado (rangos mayores)
    - raw data counts
    """
    # Raw ads (página completa según CONFIG)
    cop_buy = _fetch_ads("BUY", "COP")
    cop_sell = _fetch_ads("SELL", "COP")
    ves_buy = _fetch_ads("BUY", "VES")
    ves_sell = _fetch_ads("SELL", "VES")

    # Para tasas (remesas) usamos los primeros CONFIG["filas_tasa_remesa"]
    n = CONFIG["filas_tasa_remesa"]
    cop_buy_tasa = cop_buy[:n]
    cop_sell_tasa = cop_sell[:n]
    ves_buy_tasa = ves_buy[:n]
    ves_sell_tasa = ves_sell[:n]

    # Asegurar datos
    def _extract_prices(lista):
        try:
            return [float(x["adv"]["price"]) for x in lista if x and x.get("adv")]
        except Exception:
            return []

    precios_cop_buy = _extract_prices(cop_buy_tasa)
    precios_cop_sell = _extract_prices(cop_sell_tasa)
    precios_ves_buy = _extract_prices(ves_buy_tasa)
    precios_ves_sell = _extract_prices(ves_sell_tasa)

    # Promedios simples para tasas (remesas) — obligación tuya mantenerlos
    avg_cop_buy = sum(precios_cop_buy) / len(precios_cop_buy) if precios_cop_buy else None
    avg_cop_sell = sum(precios_cop_sell) / len(precios_cop_sell) if precios_cop_sell else None
    avg_ves_buy = sum(precios_ves_buy) / len(precios_ves_buy) if precios_ves_buy else None
    avg_ves_sell = sum(precios_ves_sell) / len(precios_ves_sell) if precios_ves_sell else None

    # Tasas obligatorias (remesas):
    tasa_cop_ves_5 = (avg_cop_buy / avg_ves_sell * 1.05) if avg_cop_buy and avg_ves_sell else None
    tasa_cop_ves_10 = (avg_cop_buy / avg_ves_sell * 1.10) if avg_cop_buy and avg_ves_sell else None
    tasa_ves_cop_5 = (avg_ves_buy / avg_cop_sell * 1.05) if avg_ves_buy and avg_cop_sell else None

    # Análisis profundo (usando todos los anuncios descargados por CONFIG["rows"])
    info_cop_buy = analizar_mercado(cop_buy, "COP", "BUY")
    info_cop_sell = analizar_mercado(cop_sell, "COP", "SELL")
    info_ves_buy = analizar_mercado(ves_buy, "VES", "BUY")
    info_ves_sell = analizar_mercado(ves_sell, "VES", "SELL")

    # Spreads para arbitraje (usamos promedios ponderados si están disponibles)
    # Para cálculo simple de oportunidad: usar avg_ponderado si existe, si no usar avg simple.
    def _choose_avg(info):
        return info.get("avg_ponderado") or info.get("avg")

    avg_cop_buy_for_arbit = _choose_avg(info_cop_buy)
    avg_cop_sell_for_arbit = _choose_avg(info_cop_sell)
    avg_ves_buy_for_arbit = _choose_avg(info_ves_buy)
    avg_ves_sell_for_arbit = _choose_avg(info_ves_sell)

    # Calculo simple de rutas de arbitraje (porcentaje potencial)
    # COP -> USDT -> VES  (comprar USDT con COP = cop_buy ; vender USDT por VES = ves_sell)
    arb_cop_to_ves = None
    if avg_cop_buy_for_arbit and avg_ves_sell_for_arbit:
        arb_cop_to_ves = (avg_ves_sell_for_arbit / avg_cop_buy_for_arbit - 1) * 100

    # VES -> USDT -> COP (comprar USDT con VES = ves_buy ; vender USDT por COP = cop_sell)
    arb_ves_to_cop = None
    if avg_ves_buy_for_arbit and avg_cop_sell_for_arbit:
        arb_ves_to_cop = (avg_cop_sell_for_arbit / avg_ves_buy_for_arbit - 1) * 100

    result = {
        "timestamps": {"utc": datetime.datetime.now(datetime.UTC).isoformat()},
        "COP": {
            "promedio_buy_tasa": avg_cop_buy,
            "promedio_sell_tasa": avg_cop_sell,
            "raw_count": len(cop_buy)
        },
        "VES": {
            "promedio_buy_tasa": avg_ves_buy,
            "promedio_sell_tasa": avg_ves_sell,
            "raw_count": len(ves_buy)
        },
        "tasas_remesas": {
            "cop_ves_5pct": tasa_cop_ves_5,
            "cop_ves_10pct": tasa_cop_ves_10,
            "ves_cop_5pct": tasa_ves_cop_5
        },
        "analisis": {
            "cop_buy": info_cop_buy,
            "cop_sell": info_cop_sell,
            "ves_buy": info_ves_buy,
            "ves_sell": info_ves_sell
        },
        "arbitraje": {
            "cop_to_ves_pct": arb_cop_to_ves,
            "ves_to_cop_pct": arb_ves_to_cop
        },
        "raw": {
            "cop_buy_raw": cop_buy,
            "cop_sell_raw": cop_sell,
            "ves_buy_raw": ves_buy,
            "ves_sell_raw": ves_sell
        }
    }

    return result

# ==============================
# FORMATTERS (mensajes para Telegram)
# ==============================

def format_compact_market(fiat: str, data: dict) -> str:
    """
    Formato compacto (para /COP y /VES).
    Incluye promedios (remesas) y tasa obligatoria en formato C (emojis).
    """
    fiat = fiat.upper()
    if fiat not in ("COP", "VES"):
        return "Par no soportado."

    # Extraer valores seguros
    if fiat == "COP":
        prom_buy = data["COP"]["promedio_buy_tasa"]
        prom_sell = data["COP"]["promedio_sell_tasa"]
        # tasas COP->VES
        t5 = data["tasas_remesas"]["cop_ves_5pct"]
        t10 = data["tasas_remesas"]["cop_ves_10pct"]
        header = "🇨🇴 *USDT ↔ COP*"
    else:
        prom_buy = data["VES"]["promedio_buy_tasa"]
        prom_sell = data["VES"]["promedio_sell_tasa"]
        # tasa VES->COP
        t5 = data["tasas_remesas"]["ves_cop_5pct"]
        t10 = None
        header = "🇻🇪 *USDT ↔ VES*"

    if prom_buy is None or prom_sell is None:
        return f"{header}\n⚠️ Datos insuficientes."

    # Mensaje compacto (formato C)
    lines = [f"{header}"]
    lines.append(f"• Compra (avg): {prom_buy:,.2f}")
    lines.append(f"• Venta  (avg): {prom_sell:,.2f}")
    lines.append(f"• Spread aproximado: {(prom_buy/prom_sell-1)*100:.2f}%")
    lines.append("")
    # tasas con emojis (obligatorias)
    if fiat == "COP":
        lines.append("💱 COP→VES:")
        lines.append(f"• +5%  → {t5:,.6f}" if t5 else "• +5%  → N/D")
        lines.append(f"• +10% → {t10:,.6f}" if t10 else "• +10% → N/D")
    else:
        lines.append("💱 VES→COP:")
        lines.append(f"• +5%  → {t5:,.6f}" if t5 else "• +5%  → N/D")

    # agregar estabilidad simple
    info_key = "cop_buy" if fiat == "COP" else "ves_buy"
    coef = data["analisis"][info_key]["coef_var"]
    stability = "⚠️ Alta volatilidad" if coef and coef > CONFIG["umbral_volatilidad"] else "✅ Estable"
    lines.append("")
    lines.append(f"🔎 Estabilidad: {stability}")

    return "\n".join(lines)

# ==============================
# NUEVO: FORMATEADOR PARA /TASA
# ==============================
def format_tasa(data: dict) -> str:
    """
    Formato compacto para el comando /TASA.
    Muestra las tasas remesas COP→VES, VES→COP y agrega:
    - Zelle a Bs. (precios_ves_sell * 0.93)
    - USDCOP (precios_cop_sell * 0.95)
    """
    tasas = data["tasas_remesas"]
    precios_ves_sell = data["VES"]["promedio_sell_tasa"]
    precios_cop_sell = data["COP"]["promedio_sell_tasa"]

    # Aplicar factores
    zelle_bs = precios_ves_sell * 0.93 if precios_ves_sell else None
    usd_cop = precios_cop_sell * 0.95 if precios_cop_sell else None

    lines = ["💱 *TASAS ACTUALES FastMoney*"]
    lines.append(f"🕒 {datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")

    # Tasas remesas
    lines.append("🇨🇴 COP → 🇻🇪 VES")
    lines.append(f"• +5%  → {tasas['cop_ves_5pct']:.6f}" if tasas['cop_ves_5pct'] else "• +5%  → N/D")
    lines.append(f"• +10% → {tasas['cop_ves_10pct']:.6f}" if tasas['cop_ves_10pct'] else "• +10% → N/D")
    lines.append("")
    lines.append("🇻🇪 VES → 🇨🇴 COP")
    lines.append(f"• +5%  → {tasas['ves_cop_5pct']:.6f}" if tasas['ves_cop_5pct'] else "• +5%  → N/D")
    lines.append("")

    # Agregar variables derivadas
    lines.append("🏦 *Tasas de referencia externas:*")
    if zelle_bs:
        lines.append(f"• Zelle → Bs.: {zelle_bs:,.2f}")
    else:
        lines.append("• Zelle → Bs.: N/D")

    if usd_cop:
        lines.append(f"• USDCOP: {usd_cop:,.2f}")
    else:
        lines.append("• USDCOP: N/D")

    return "\n".join(lines)


# ==============================
# NUEVO COMANDO: /TASA
# ==============================
async def cmd_tasa(update, context):
    """Obtiene los datos actuales y muestra las tasas y referencias."""
    await update.message.reply_text("⏳ Consultando tasas y referencias...")
    data = await _get_data_async()
    msg = format_tasa(data)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


def format_arbitraje(data: dict) -> str:
    """
    Formato analítico para /ARBITRAJE:
    Muestra ambas rutas, porcentajes y recomienda la mejor (si aplica).
    """
    arb_c = data["arbitraje"]["cop_to_ves_pct"]
    arb_v = data["arbitraje"]["ves_to_cop_pct"]

    header = "📊 *ARBITRAJE — Evaluación de rutas*\n"
    lines = [header]

    def pretty(p):
        return f"{p:.2f}%"

    if arb_c is None and arb_v is None:
        lines.append("⚠️ Datos insuficientes para evaluar arbitraje.")
        return "\n".join(lines)

    # Mostrar COP -> VES
    if arb_c is not None:
        lines.append(f"• COP → USDT → VES: {pretty(arb_c)} {'✅ Rentable' if arb_c>0 else '❌ No rentable'}")
    else:
        lines.append("• COP → ... : N/D")

    # Mostrar VES -> COP
    if arb_v is not None:
        lines.append(f"• VES → USDT → COP: {pretty(arb_v)} {'✅ Rentable' if arb_v>0 else '❌ No rentable'}")
    else:
        lines.append("• VES → ... : N/D")

    # Recomendación simple
    best = None
    if arb_c is not None and arb_v is not None:
        if arb_c > arb_v and arb_c > 0:
            best = "COP → VES"
        elif arb_v > arb_c and arb_v > 0:
            best = "VES → COP"
    elif arb_c is not None and arb_c > 0:
        best = "COP → VES"
    elif arb_v is not None and arb_v > 0:
        best = "VES → COP"

    lines.append("")
    if best:
        lines.append(f"🔔 *Recomendación:* Mejor ruta actual: {best}")
    else:
        lines.append("🔕 No hay una ruta claramente rentable ahora mismo.")

    # Añadir algunos indicadores útiles (liquidez detectada aprox.)
    # estimamos liquidez disponible como suma de dynamicMaxSingleTransAmount en raw (simple)
    try:
        cop_liq = sum(float(x["adv"].get("dynamicMaxSingleTransAmount", 0) or 0) for x in data["raw"]["cop_buy_raw"])
        ves_liq = sum(float(x["adv"].get("dynamicMaxSingleTransAmount", 0) or 0) for x in data["raw"]["ves_buy_raw"])
        lines.append(f"\n💧 Liquidez estimada (USDT): COP-side {cop_liq:,.2f} | VES-side {ves_liq:,.2f}")
    except Exception:
        pass

    return "\n".join(lines)

def format_all(data: dict) -> str:
    """Reporte completo (versión A) con secciones COP, VES, Arbitraje y métricas."""
    # COP section
    cop = data["COP"]
    ves = data["VES"]
    info = data["analisis"]
    tasas = data["tasas_remesas"]

    lines = []
    lines.append("📊 *REPORTE COMPLETO*")
    lines.append(f"🕒 {datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("\n🇨🇴 COP — Mercado")
    lines.append(f"• Compra (avg): {cop['promedio_buy_tasa']:,.2f}" if cop['promedio_buy_tasa'] else "• Compra (avg): N/D")
    lines.append(f"• Venta  (avg): {cop['promedio_sell_tasa']:,.2f}" if cop['promedio_sell_tasa'] else "• Venta  (avg): N/D")
    lines.append(f"• Promedio ampliado (ponderado): {info['cop_buy']['avg_ponderado']:,.6f}")
    lines.append(f"• Volatilidad (coef_var): {info['cop_buy']['coef_var']:.4f}%")
    lines.append(f"• Outliers detectados: {info['cop_buy']['outliers']}")
    lines.append("")
    lines.append("💱 COP→VES (remesas):")
    lines.append(f"• +5%  → {tasas['cop_ves_5pct']:.6f}" if tasas['cop_ves_5pct'] else "• +5%  → N/D")
    lines.append(f"• +10% → {tasas['cop_ves_10pct']:.6f}" if tasas['cop_ves_10pct'] else "• +10% → N/D")
    lines.append("\n🇻🇪 VES — Mercado")
    lines.append(f"• Compra (avg): {ves['promedio_buy_tasa']:,.2f}" if ves['promedio_buy_tasa'] else "• Compra (avg): N/D")
    lines.append(f"• Venta  (avg): {ves['promedio_sell_tasa']:,.2f}" if ves['promedio_sell_tasa'] else "• Venta  (avg): N/D")
    lines.append(f"• Promedio ampliado (ponderado): {info['ves_sell']['avg_ponderado']:,.6f}")
    lines.append(f"• Volatilidad (coef_var): {info['ves_sell']['coef_var']:.4f}%")
    lines.append(f"• Outliers detectados: {info['ves_sell']['outliers']}")
    lines.append("")
    lines.append("📊 ARBITRAJE")
    arb = data["arbitraje"]
    lines.append(f"• COP→VES: {arb['cop_to_ves_pct']:.2f}%" if arb['cop_to_ves_pct'] is not None else "• COP→VES: N/D")
    lines.append(f"• VES→COP: {arb['ves_to_cop_pct']:.2f}%" if arb['ves_to_cop_pct'] is not None else "• VES→COP: N/D")
    lines.append("")
    lines.append("🔎 Notas:")
    lines.append("- Promedios de remesas usan los primeros {} anuncios".format(CONFIG["filas_tasa_remesa"]))
    lines.append("- Promedios ampliados y ponderados usan todos los anuncios descargados por CONFIG (rows).")
    lines.append("")
    return "\n".join(lines)

# ==============================
# Comandos Telegram (async)
# ==============================

async def cmd_start(update, context: ContextTypes.DEFAULT_TYPE):
    texto = (
        "👋 Hola — Bot P2P activo.\n"
        "Comandos:\n"
        "/TASA → Resumen COP (compacto)\n"
        "/COP → Resumen COP (compacto)\n"
        "/VES → Resumen VES (compacto)\n"
        "/ARBITRAJE → Análisis de oportunidades\n"
        "/ALL → Reporte completo\n"
        "/ACT → Forzar actualización y enviar reporte completo\n"
        "/auto_on → Encender envíos automáticos al CHAT_ID\n"
        "/auto_off → Apagar envíos automáticos\n"
    )
    await update.message.reply_text(texto)

# utility to call sync function in threadpool
async def _get_data_async():
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, get_p2p_data)

async def cmd_cop(update, context):
    await update.message.reply_text("⏳ Consultando mercado COP...")
    data = await _get_data_async()
    msg = format_compact_market("COP", data)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def cmd_ves(update, context):
    await update.message.reply_text("⏳ Consultando mercado VES...")
    data = await _get_data_async()
    msg = format_compact_market("VES", data)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def cmd_arbitraje(update, context):
    await update.message.reply_text("⏳ Analizando rutas de arbitraje...")
    data = await _get_data_async()
    msg = format_arbitraje(data)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def cmd_all(update, context):
    await update.message.reply_text("⏳ Generando reporte completo...")
    data = await _get_data_async()
    msg = format_all(data)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def cmd_act(update, context):
    await update.message.reply_text("⏳ Forzando actualización y enviando reporte completo...")
    data = await _get_data_async()
    msg = format_all(data)
    # enviar al CHAT_ID configurado además de confirmar al usuario
    try:
        await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN)
        await update.message.reply_text("✅ Enviado al canal configurado (CHAT_ID).")
    except Exception as e:
        await update.message.reply_text(f"❌ Error enviando al CHAT_ID: {e}")

async def cmd_auto_on(update, context):
    """
    Activa los envíos automáticos de tasas.
    Uso: /auto_on [segundos]
    Si no se indica valor, usa el intervalo del archivo CONFIG.
    """
    global auto_job

    # 🧠 Evita múltiples jobs activos
    if auto_job:
        await update.message.reply_text("✅ El modo automático ya está activo.")
        return

    # ⏱️ Intervalo configurable (segundos)
    try:
        if context.args:  # Si el usuario envía algo como /auto_on 300
            interval = int(context.args[0])
        else:
            interval = CONFIG.get("intervalo_tiempo", 600)  # valor por defecto 10 min
    except ValueError:
        await update.message.reply_text("⚠️ Intervalo inválido. Ejemplo: /auto_on 300")
        return

    minutes = max(1, int(interval / 60))

    # 🧩 Nueva tarea: enviar solo las tasas (modo remesas)
    async def job_send():
        try:
            data = await _get_data_async()
            msg = format_tasa(data)  # 📊 solo tasas, sin arbitraje
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=msg,
                parse_mode="HTML"
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=f"⚠️ Error en job automático: {e}"
            )

    # APScheduler trabaja con funciones sync, así que usamos run_in_executor
    def wrapper_job():
        loop = asyncio.get_event_loop()
        loop.create_task(job_send())

    # 🗓️ Registrar tarea
    auto_job = scheduler.add_job(wrapper_job, "interval", seconds=interval)
    await update.message.reply_text(f"💱 Envíos automáticos ACTIVADOS cada {minutes} minuto(s).")


async def cmd_auto_off(update, context):
    """Desactiva el modo automático."""
    global auto_job
    if auto_job:
        auto_job.remove()
        auto_job = None
        await update.message.reply_text("🛑 Envíos automáticos desactivados.")
    else:
        await update.message.reply_text("⚠️ No había envíos automáticos activos.")

# ==============================
# BOOT / MAIN
# ==============================
_GLOBAL_APP_REF = None

def main():
    global _GLOBAL_APP_REF
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    _GLOBAL_APP_REF = app

    # Registrar handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("TASA", cmd_tasa))
    app.add_handler(CommandHandler("COP", cmd_cop))
    app.add_handler(CommandHandler("VES", cmd_ves))
    app.add_handler(CommandHandler("ARBITRAJE", cmd_arbitraje))
    app.add_handler(CommandHandler("ALL", cmd_all))
    app.add_handler(CommandHandler("ACT", cmd_act))
    app.add_handler(CommandHandler("auto_on", cmd_auto_on))
    app.add_handler(CommandHandler("auto_off", cmd_auto_off))

    print("🤖 Bot iniciado (modo escucha, PTB v20+).")
    app.run_polling()

if __name__ == "__main__":
    main()
