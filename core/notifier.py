import logging
import pytz 
from datetime import datetime
from core import pipeline, db
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
hora_colombia = datetime.now(tz_colombia)


def format_tasa(data: dict) -> str:
    """Formato compacto tipo /TASA (texto plano)."""
    tasas = data.get("tasas_remesas", {})
    precios_ves_sell = data.get("VES", {}).get("promedio_sell_tasa")
    precios_ves_buy = data.get("VES", {}).get("promedio_buy_tasa")
    precios_cop_sell = data.get("COP", {}).get("promedio_sell_tasa")
    precios_cop_buy = data.get("COP", {}).get("promedio_buy_tasa")

    zelle_bs = precios_ves_sell * 0.915 if precios_ves_sell else None
    bs_zelle = precios_ves_buy * 1.05 if precios_ves_buy else None
    usd_cop_buy = precios_cop_sell * 0.95 if precios_cop_sell else None
    usd_cop_sell = precios_cop_buy * 1.05 if precios_cop_buy else None

    lines = []
    lines.append("💱 FASTMONEY — TASAS ACTUALES")
    lines.append(f"🕒 {hora_colombia.strftime('%Y-%m-%d %H:%M:%S')} (Colombia)")
    lines.append("")
    lines.append("🇨🇴 COP → 🇻🇪 VES")
    lines.append(f"• {tasas.get('cop_ves_10pct'):.2f}" if tasas.get('cop_ves_10pct') else "• +10% → N/D")
    lines.append("")
    lines.append("🇻🇪 VES → 🇨🇴 COP")
    lines.append(f"• {tasas.get('ves_cop_5pct'):.4f}" if tasas.get('ves_cop_5pct') else "• +5% → N/D")
    lines.append("")
    if zelle_bs:
        lines.append(f"• Zelle → Bs.: {zelle_bs:,.0f}")
    else:
        lines.append("• Zelle → Bs.: N/D")
    if bs_zelle:
        lines.append(f"• Bs. → Zelle: {bs_zelle:,.0f}")
    else:        
        lines.append("• Bs. → Zelle: N/D") 
    lines.append("")
    if usd_cop_buy:
        lines.append(f"• Compra USDCOP: {usd_cop_buy:,.0f}")
    else:
        lines.append("• Compra USDCOP: N/D")
    if usd_cop_sell:
        lines.append(f"• Venta USDCOP: {usd_cop_sell:,.0f}")
    else:
        lines.append("• Venta USDCOP: N/D")

    lines.append("")
    lines.append("AVISO IMPORTANTE: ")
    lines.append("No garantizamos la exactitud de estos datos. Usted es el unico responsasble de las decisiones que tome basandose en esta informacion.")
    lines.append("El administrador del canal no se hace responsable por perdidas o daños derivados del uso de esta informacion.")
    lines.append("")
    lines.append("atte. FastMoney.")
    lines.append("")

    return "\n".join(lines)


def format_compact_market(fiat: str, data: dict) -> str:
    fiat = fiat.upper()
    if fiat not in ("COP", "VES"):
        return "Par no soportado."
    if fiat == "COP":
        prom_buy = data["COP"]["promedio_buy_tasa"]
        prom_sell = data["COP"]["promedio_sell_tasa"]
        t5 = data["tasas_remesas"].get("cop_ves_5pct")
        t10 = data["tasas_remesas"].get("cop_ves_10pct")
        header = "🇨🇴 USDT ↔ COP"
        info_key = "cop_buy"
    else:
        prom_buy = data["VES"]["promedio_buy_tasa"]
        prom_sell = data["VES"]["promedio_sell_tasa"]
        t5 = data["tasas_remesas"].get("ves_cop_5pct")
        t10 = None
        header = "🇻🇪 USDT ↔ VES"
        info_key = "ves_buy"

    if prom_buy is None or prom_sell is None:
        return f"{header}\n⚠️ Datos insuficientes."

    lines = [header]
    try:
        lines.append(f"• Compra (avg): {prom_buy:,.2f}")
        lines.append(f"• Venta  (avg): {prom_sell:,.2f}")
        lines.append(f"• Spread aproximado: {(prom_buy/prom_sell-1)*100:.2f}%")
    except Exception:
        pass
    lines.append("")
    if fiat == "COP":
        lines.append("💱 COP→VES:")
        lines.append(f"• +5%  → {t5:,.6f}" if t5 else "• +5%  → N/D")
        lines.append(f"• +10% → {t10:,.6f}" if t10 else "• +10% → N/D")
    else:
        lines.append("💱 VES→COP:")
        lines.append(f"• +5%  → {t5:,.6f}" if t5 else "• +5%  → N/D")

    coef = data["analisis"].get(info_key, {}).get("coef_var")
    stability = "⚠️ Alta volatilidad" if coef and coef > 3 else "✅ Estable"
    lines.append("")
    lines.append(f"🔎 Estabilidad: {stability}")

    return "\n".join(lines)


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
        loop.run_until_complete(bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode))
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
        log.warning("apscheduler not available; notifier scheduler disabled (fallback).")

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
    log.info(f"Notifier scheduler iniciado (every {interval}s, dry_run={dry_run})")
    return sched
