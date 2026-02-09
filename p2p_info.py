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
from core import pipeline

# ==============================
# CONFIGURACIÓN GLOBAL (Centralizada)
# ==============================
CONFIG = {
    "pares": ["USDT-COP", "USDT-VES"],
    "monedas": {
        "COP": {"rows": 20, "page": 2},  # usas página 2 por tu preferencia
        "VES": {"rows": 20, "page": 4}
    },
    "horas_programadas": [6, 8, 10, 14, 18, 20, 22],
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
OWNER_ID = os.getenv("OWNER_ID") 
if not TELEGRAM_TOKEN or not CHAT_ID or not OWNER_ID:
    raise ValueError("Faltan TELEGRAM_TOKEN, CHAT_ID o OWNER_ID en .env")

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
    """Construye los datos a partir de lo almacenado en la DB (pipeline).

    Esta función delega en `core.pipeline.build_data_from_db(CONFIG)` para
    producir la misma estructura que antes, pero leyendo la última respuesta
    guardada por mercado/tradeType.
    """
    try:
        return pipeline.build_data_from_db(CONFIG)
    except Exception as e:
        print(f"⚠️ Error leyendo desde DB en pipeline: {e}")
        return {}

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
    precios_cop_buy = data["COP"]["promedio_buy_tasa"]

    # Aplicar factores
    zelle_bs = precios_ves_sell * 0.90 if precios_ves_sell else None
    usd_cop_buy = precios_cop_sell * 0.95 if precios_cop_sell else None
    usd_cop_sell = precios_cop_buy * 1.05 if precios_cop_sell else None

    lines = ["💱 *FASTMONEY - TASAS ACTUALES*"]
    lines.append(f"🕒 {datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")

    # Tasas remesas
    lines.append("🇨🇴 COP → 🇻🇪 VES")
    lines.append(f"• → {tasas['cop_ves_10pct']:.1f}" if tasas['cop_ves_10pct'] else "• +10% → N/D")
    lines.append("")
    lines.append("🇻🇪 VES → 🇨🇴 COP")
    lines.append(f"•  → {tasas['ves_cop_5pct']:.5f}" if tasas['ves_cop_5pct'] else "• +5%  → N/D")
    lines.append("")

    # Agregar variables derivadas
    lines.append("🏦 *Tasas de referencia externas:*")
    lines.append("")
    if zelle_bs:
        lines.append(f"• Zelle → Bs.: {zelle_bs:,.0f}")
    else:
        lines.append("• Zelle → Bs.: N/D")

    lines.append("")

    if usd_cop_buy:
        lines.append(f"• Compra USDCOP: {usd_cop_buy:,.0f}")
    else:
        lines.append("• Compra USDCOP: N/D")

    if usd_cop_sell:
        lines.append(f"• Venta USDCOP: {usd_cop_sell:,.0f}")
    else:
        lines.append("• Venta USDCOP: N/D")

    return "\n".join(lines)

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
    return "\n".join(lines)

# ==============================
# Comandos Telegram (async)
# ==============================

async def cmd_start(update, context: ContextTypes.DEFAULT_TYPE):
    # Detecta el chat correcto, sea grupo, canal o privado
    chat_id = (
        update.effective_chat.id
        if update.effective_chat
        else getattr(update.channel_post, "chat_id", None)
    )

    texto = (
        "👋 Bienvenidos a FASTMONEY.\n"
        "Comandos:\n"
        "/TASA → Resumen de mercado\n"
#        "/COP → Resumen COP (compacto)\n"
#        "/VES → Resumen VES (compacto)\n"
#        "/ARBITRAJE → Análisis de oportunidades\n"
#        "/ALL → Reporte completo\n"
#        "/ACT → Forzar actualización y enviar reporte completo\n"
#        "/auto_on → Encender envíos automáticos al canal oficial\n"
#        "/auto_off → Apagar envíos automáticos\n"
    )

    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text=texto)

# utility to call sync function in threadpool
async def _get_data_async():
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, get_p2p_data)

async def cmd_tasa(update, context):
    """Obtiene los datos actuales y muestra las tasas y referencias."""
    chat_id = (
        update.effective_chat.id
        if update.effective_chat
        else getattr(update.channel_post, "chat_id", None)
    )

    if not chat_id:
        return

    await context.bot.send_message(chat_id=chat_id, text="⏳ Consultando tasas y referencias...")

    data = await _get_data_async()
    msg = format_tasa(data)

    await context.bot.send_message(
        chat_id=chat_id,
        text=msg,
        parse_mode=ParseMode.MARKDOWN
    )
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
    await update.message.reply_text("⏳ Forzando actualización y enviando tasa")
    data = await _get_data_async()
    msg = format_tasa(data)
    # enviar al CHAT_ID configurado además de confirmar al usuario
    try:
        await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN)
        await update.message.reply_text("✅ Enviado al canal configurado (CHAT_ID).")
    except Exception as e:
        await update.message.reply_text(f"❌ Error enviando al CHAT_ID: {e}")

async def cmd_auto_on(update, context):
    """Activa los envíos automáticos de tasas (solo canal oficial)."""
    chat_id = update.effective_chat.id
    if str(chat_id) != str(OWNER_ID):
        await update.message.reply_text("⚠️ Este comando solo está disponible para el administrador.")
        return

    # ⏱️ Intervalo configurable
    try:
        interval = int(context.args[0]) if context.args else CONFIG.get("intervalo_tiempo", 600)
    except ValueError:
        await update.message.reply_text("⚠️ Intervalo inválido. Ejemplo: /auto_on 300")
        return

    # Verifica si ya hay un job
    if context.job_queue.get_jobs_by_name("auto_tasa"):
        await update.message.reply_text("✅ El modo automático ya está activo.")
        return

    # 🧩 Define el job
    async def job_send(context):
        try:
            data = await _get_data_async()
            msg = format_tasa(data)
            await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            await context.bot.send_message(chat_id=OWNER_ID, text=f"⚠️ Error en job automático: {e}")

    # 🗓️ Registra el job
    context.job_queue.run_repeating(job_send, interval=interval, first=5, name="auto_tasa")

    minutes = max(1, int(interval / 60))
    await update.message.reply_text(f"💱 Envíos automáticos ACTIVADOS cada {minutes} minuto(s).")

# ------------------------------------------------------------
# /auto_off → solo para el canal oficial
# ------------------------------------------------------------
async def cmd_auto_off(update, context):
    """Desactiva los envíos automáticos."""
    if str(update.effective_chat.id) != str(OWNER_ID):
        await update.message.reply_text("⚠️ Solo el administrador puede usar este comando.")
        return

    jobs = context.job_queue.get_jobs_by_name("auto_tasa")
    if not jobs:
        await update.message.reply_text("⚠️ No hay tareas automáticas activas.")
        return

    for job in jobs:
        job.schedule_removal()

    await update.message.reply_text("🛑 Envíos automáticos desactivados.")

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
