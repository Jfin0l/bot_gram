import logging
import pytz
from datetime import datetime
from core import pipeline, db
from core.processor import format_num, format_vol, ai_meta
from config import BOT_TOKEN, CHAT_ID, OWNER_ID, log
try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:
    BackgroundScheduler = None
try:
    from telegram import Bot
    from telegram.constants import ParseMode
except Exception:
    Bot = None
    ParseMode = None

logg = logging.getLogger(__name__)

tz_colombia = pytz.timezone('America/Bogota')


def format_tasa(data: dict) -> str:
    """Formato compacto tipo /TASA (texto plano)."""
    tasas = data.get("tasas_remesas", {})
    precios_ves_sell = data.get("VES", {}).get("promedio_sell_tasa")
    precios_ves_buy = data.get("VES", {}).get("promedio_buy_tasa")
    precios_cop_sell = data.get("COP", {}).get("promedio_sell_tasa")
    precios_cop_buy = data.get("COP", {}).get("promedio_buy_tasa")

    # Metodo Zelle: Ajustado a 5% de margen (Buy +5%, Sell -5%)
    zelle_bs = precios_ves_sell * 0.93 if precios_ves_sell else None
    bs_zelle = precios_ves_buy * 1.05 if precios_ves_buy else None

    # Metodo USD-COP: Manteniendo el 5% de margen solicitado
    usd_cop_buy = precios_cop_sell * 0.95 if precios_cop_sell else None
    usd_cop_sell = precios_cop_buy * 1.05 if precios_cop_buy else None

    lines = []
    lines.append("💎 <b>FASTMONEY — TASAS OFICIALES</b>")
    hora_colombia = datetime.now(tz_colombia)
    lines.append(
        f"🕒 <code>{hora_colombia.strftime('%Y-%m-%d %H:%M:%S')}</code> (Colombia)")
    lines.append("")

    lines.append("🇨🇴 <b>OPERACIONES COP → VES</b>")
    lines.append(f"• Tasa Estándar: <b>{format_num(tasas.get('cop_ves_10pct'), 2)}</b>" if tasas.get(
        'cop_ves_10pct') else "• Tasa Estándar → N/D")
    # lines.append(f"• Tasa Preferencial (+5%): <b>{format_num(tasas.get('cop_ves_5pct'), 2)}</b>" if tasas.get(
    #    'cop_ves_5pct') else "• +5% → N/D")
    lines.append("")

    lines.append("🇻🇪 <b>OPERACIONES VES → COP</b>")
    lines.append(f"• Tasa Retorno: <b>{format_num(tasas.get('ves_cop_5pct'), 4)}</b>" if tasas.get(
        'ves_cop_5pct') else "• Tasa Retorno → N/D")
    lines.append("")

    lines.append("📱 <b>MÉTODOS DIGITALES</b>")
    if zelle_bs:
        lines.append(
            f"• Compra Zelle (Cobra Bs): <b>{format_num(zelle_bs, 0)}</b>")
    else:
        lines.append("• Compra Zelle: N/D")

    if bs_zelle:
        lines.append(
            f"• Venta Zelle (Paga Bs): <b>{format_num(bs_zelle, 0)}</b>")
    else:
        lines.append("• Venta Zelle: N/D")
    lines.append("")

    lines.append("💵 <b>CAMBIO USD/COP</b>")
    if usd_cop_buy:
        lines.append(f"• Compra USDCOP: <b>{format_num(usd_cop_buy, 0)}</b>")
    else:
        lines.append("• Compra USDCOP: N/D")
    if usd_cop_sell:
        lines.append(f"• Venta USDCOP: <b>{format_num(usd_cop_sell, 0)}</b>")
    else:
        lines.append("• Venta USDCOP: N/D")

    lines.append("")
    lines.append("⚠️ <b>NOTAS DE MERCADO:</b>")
    lines.append(
        "Nota: los datos aqui presentados son solo de referencia, para mayor informacion contacte a un operador.")
    lines.append("")
    lines.append("— <i>Atte. FastMoney Systems</i>")

    meta = {
        "type": "official_rates",
        "timestamp": hora_colombia.isoformat(),
        "cop_ves_10": tasas.get('cop_ves_10pct'),
        "ves_cop_5": tasas.get('ves_cop_5pct'),
        "zelle_buy": zelle_bs,
        "zelle_sell": bs_zelle
    }

    return "\n".join(lines) + ai_meta(meta)


def format_compact_market(fiat: str, data: dict) -> str:
    fiat = fiat.upper()

    # Defaults and symbols
    flag = "💰"
    if fiat == "COP":
        flag = "🇨🇴"
    elif fiat == "VES":
        flag = "🇻🇪"
    elif fiat == "ARS":
        flag = "🇦🇷"
    elif fiat == "BRL":
        flag = "🇧🇷"

    header = f"{flag} USDT ↔ {fiat}"

    market_data = data.get(fiat)
    if not market_data:
        return f"{header}\n⚠️ Sin datos para esta moneda."

    prom_buy = market_data.get("promedio_buy_tasa")
    prom_sell = market_data.get("promedio_sell_tasa")

    if prom_buy is None or prom_sell is None:
        return f"{header}\n⚠️ Datos insuficientes para {fiat}."

    lines = [f"📊 <b>{header}</b>"]
    try:
        lines.append(f"• Compra (avg): <b>{format_num(prom_buy)}</b>")
        lines.append(f"• Venta  (avg): <b>{format_num(prom_sell)}</b>")
        # Spread: (Compra - Venta) / Venta
        lines.append(f"• Spread: <b>{(prom_buy/prom_sell-1)*100:.2f}%</b>")
    except Exception as e:
        logg.warning("Error calculando spread en format_compact_market: %s", e)

    # Remesas logic only if we have the pair COP/VES logic defined
    tasas_remesas = data.get("tasas_remesas", {})
    if fiat == "COP" and "cop_ves_5pct" in tasas_remesas:
        lines.append("")
        lines.append("💱 <b>COP→VES Remesas:</b>")
        t5 = tasas_remesas.get("cop_ves_5pct")
        t10 = tasas_remesas.get("cop_ves_10pct")
        lines.append(
            f"• Tasa preferencial → <b>{format_num(t5, 2)}</b>" if t5 else "• → N/D")
        lines.append(
            f"• Tasa estándar → <b>{format_num(t10, 2)}</b>" if t10 else "• → N/D")
    elif fiat == "VES" and "ves_cop_5pct" in tasas_remesas:
        lines.append("")
        lines.append("💱 <b>VES→COP Remesas:</b>")
        t5 = tasas_remesas.get("ves_cop_5pct")
        lines.append(
            f"• Tasa retorno → <b>{format_num(t5, 4)}</b>" if t5 else "• → N/D")

    info_key = f"{fiat.lower()}_buy"
    coef = data.get("analisis", {}).get(info_key, {}).get("coef_var")
    if coef is not None:
        stability = "⚠️ Alta volatilidad" if coef > 3 else "✅ Estable"
        lines.append("")
        lines.append(f"🔎 <b>Estabilidad:</b> {stability} (CV: {coef:.2f})")

    meta = {
        "type": "compact_market",
        "fiat": fiat,
        "avg_buy": prom_buy,
        "avg_sell": prom_sell,
        "stability": stability
    }
    return "\n".join(lines) + ai_meta(meta)


def send_message(chat_id: str, text: str, parse_mode: str = "HTML", dry_run: bool = False):
    """Envía mensaje a `chat_id`. Si `dry_run` es True imprime en consola en vez de enviar."""
    if dry_run or not BOT_TOKEN:
        log.info("[dry-run] Mensaje a %s:\n%s", chat_id, text)
        return True

    if Bot is None:
        log.warning("python-telegram-bot no está instalado; modo dry-run")
        log.info("[dry-run] Mensaje a %s:\n%s", chat_id, text)
        return True

    try:
        import asyncio
        bot = Bot(token=BOT_TOKEN)
        # Usar un event loop para ejecutar la corrutina de forma síncrona
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bot.send_message(
            chat_id=chat_id, text=text, parse_mode=parse_mode))
        loop.close()
        return True
    except Exception as e:
        log.exception(f"Error enviando mensaje a {chat_id}: {e}")
        return False


def send_tasa_to_channel(config: dict, chat_id: str = None, dry_run: bool = False):
    data = pipeline.build_data_from_db(config)
    if not data:
        log.warning("No hay datos para enviar tasa")
        return False
    text = format_tasa(data)
    target = chat_id or CHAT_ID
    return send_message(target, text, parse_mode="HTML", dry_run=dry_run)


def start_notifier_scheduler(config: dict, interval: int = 3600, chat_id: str = None, dry_run: bool = False):
    if BackgroundScheduler is None:
        log.warning(
            "apscheduler not available; notifier scheduler disabled (fallback).")

        class Dummy:
            def shutdown(self):
                pass

        return Dummy()

    sched = BackgroundScheduler()

    def job():
        try:
            send_tasa_to_channel(config, chat_id=chat_id, dry_run=dry_run)
        except Exception as e:
            log.exception(f"Notifier job error: {e}")

    sched.add_job(job, "interval", seconds=interval, id="notifier_tasa")
    sched.start()
    log.info(
        f"Notifier scheduler iniciado (every {interval}s, dry_run={dry_run})")
    return sched
