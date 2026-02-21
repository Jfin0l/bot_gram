"""
Telegram Bot entrypoint — Integración completa con nueva arquitectura DB.
Comandos: /TASA, /COP, /VES, /ARBITRAJE, /auto_on, /auto_off, /start
"""
import logging
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv optional in test environments
    pass
try:
    from telegram import Update, Bot
    from telegram.ext import (
        ApplicationBuilder,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
    from telegram.constants import ParseMode
    TELEGRAM_AVAILABLE = True
except Exception:
    TELEGRAM_AVAILABLE = False

from datetime import datetime
from core import pipeline, notifier, db, scheduler
import asyncio
from services.analytics.spread import handle_spread
from services.analytics.merchant import handle_merchant
from services.analytics.volatility import handle_volatility

# load .env if available (already attempted above but ensure env vars present)
try:
    from dotenv import load_dotenv as _ld
    _ld()
except Exception:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OWNER_ID = os.getenv("OWNER_ID")

if TELEGRAM_AVAILABLE and (not BOT_TOKEN or not CHAT_ID):
    raise ValueError("BOT_TOKEN y CHAT_ID requeridos en .env")

# CONFIG
CONFIG = {
    "pares": ["USDT-COP", "USDT-VES"],
    "monedas": {"COP": {"rows": 20, "page": 2}, "VES": {"rows": 20, "page": 4}},
    "filas_tasa_remesa": 5,
    "ponderacion_volumen": True,
    "limite_outlier": 0.025,
    "umbral_volatilidad": 3,
}

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Enable verbose logging for telegram to help debug incoming updates
logging.getLogger("telegram").setLevel(logging.DEBUG)


# ==============================
# HANDLERS
# ==============================

async def cmd_start(update, context):
    """Comando /start — información del bot."""
    texto = (
        "👋 *Bienvenidos a FASTMONEY*\n\n"
        "Comandos disponibles:\n"
        "/TASA — Resumen de tasas actuales\n"
        "/COP — Mercado USDT-COP\n"
        "/VES — Mercado USDT-VES\n"
        "/ARBITRAJE — Análisis de oportunidades\n"
        "/auto_on — Activar envíos automáticos cada 60 min\n"
        "/auto_off — Desactivar envíos automáticos"
    )
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=texto,
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_tasa(update, context):
    """Comando /TASA — Envía tasas actuales."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text="⏳ Consultando tasas...")
    
    try:
        data = pipeline.build_data_from_db(CONFIG)
        if not data.get("tasas_remesas"):
            await context.bot.send_message(chat_id=chat_id, text="⚠️ Sin datos disponibles")
            return
        
        msg = notifier.format_tasa(data)
        await context.bot.send_message(
            chat_id=chat_id,
            text=msg,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.exception(f"Error en /TASA: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error: {e}")


async def cmd_cop(update, context):
    """Comando /COP — Mercado COP."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text="⏳ Consultando COP...")
    
    try:
        data = pipeline.build_data_from_db(CONFIG)
        msg = notifier.format_compact_market("COP", data)
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception(f"Error en /COP: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error: {e}")


async def cmd_ves(update, context):
    """Comando /VES — Mercado VES."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text="⏳ Consultando VES...")
    
    try:
        data = pipeline.build_data_from_db(CONFIG)
        msg = notifier.format_compact_market("VES", data)
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception(f"Error en /VES: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error: {e}")


async def cmd_arbitraje(update, context):
    """Comando /ARBITRAJE — Análisis de arbitraje."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text="⏳ Analizando arbitraje...")
    
    try:
        data = pipeline.build_data_from_db(CONFIG)
        arb = data.get("arbitraje", {})
        
        lines = ["📊 *ARBITRAJE — Evaluación de rutas*"]
        cop_ves = arb.get("cop_to_ves_pct")
        ves_cop = arb.get("ves_to_cop_pct")
        
        if cop_ves is not None:
            status = "✅ Rentable" if cop_ves > 0 else "❌ No rentable"
            lines.append(f"• COP → VES: {cop_ves:.2f}% {status}")
        if ves_cop is not None:
            status = "✅ Rentable" if ves_cop > 0 else "❌ No rentable"
            lines.append(f"• VES → COP: {ves_cop:.2f}% {status}")
        
        if cop_ves is None and ves_cop is None:
            lines.append("⚠️ Datos insuficientes")
        else:
            best = None
            if cop_ves is not None and ves_cop is not None:
                best = "COP → VES" if cop_ves > ves_cop and cop_ves > 0 else ("VES → COP" if ves_cop > 0 else None)
            elif cop_ves is not None and cop_ves > 0:
                best = "COP → VES"
            elif ves_cop is not None and ves_cop > 0:
                best = "VES → COP"
            
            if best:
                lines.append(f"\n🔔 *Mejor ruta: {best}*")
        
        msg = "\n".join(lines)
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.exception(f"Error en /ARBITRAJE: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error: {e}")


async def cmd_auto_on(update, context):
    """Comando /auto_on — Activar envíos automáticos."""
    # Verificar permisos (solo OWNER_ID)
    if str(update.effective_chat.id) != str(OWNER_ID):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ Este comando solo está disponible para el administrador."
        )
        return
    
    # Verificar si ya hay un job activo
    jobs = context.job_queue.get_jobs_by_name("auto_tasa")
    if jobs:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✅ El modo automático ya está activo."
        )
        return
    
    # Obtener intervalo (default 3600s = 1 hora)
    try:
        interval = int(context.args[0]) if context.args else 3600
    except (ValueError, IndexError):
        interval = 3600
    
    # Job
    async def job_send_tasa(context):
        try:
            data = pipeline.build_data_from_db(CONFIG)
            msg = notifier.format_tasa(data)
            await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
            logger.info(f"Auto-tasa enviada a {CHAT_ID}")
        except Exception as e:
            logger.exception(f"Error en job auto_tasa: {e}")
            try:
                await context.bot.send_message(
                    chat_id=OWNER_ID,
                    text=f"⚠️ Error en auto-tasa: {e}"
                )
            except Exception:
                pass
    
    # Registrar job
    context.job_queue.run_repeating(job_send_tasa, interval=interval, first=5, name="auto_tasa")
    
    minutes = max(1, int(interval / 60))
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"💱 Envíos automáticos ACTIVADOS cada {minutes} minuto(s)."
    )
    logger.info(f"Auto-tasa activado cada {interval}s")


async def cmd_auto_off(update, context):
    """Comando /auto_off — Desactivar envíos automáticos."""
    # Verificar permisos
    if str(update.effective_chat.id) != str(OWNER_ID):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ Solo el administrador puede usar este comando."
        )
        return
    
    jobs = context.job_queue.get_jobs_by_name("auto_tasa")
    if not jobs:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ No hay tareas automáticas activas."
        )
        return
    
    for job in jobs:
        job.schedule_removal()
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🛑 Envíos automáticos desactivados."
    )
    logger.info("Auto-tasa desactivado")


async def cmd_buckets(update, context):
    """Comando /BUCKETS [pair] [n] — Muestra los últimos n buckets agregados (10m)."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text="⏳ Consultando buckets recientes...")

    # args: pair, n
    try:
        pair = (context.args[0].upper() if context.args else "USDT-COP")
    except Exception:
        pair = "USDT-COP"
    try:
        n = int(context.args[1]) if len(context.args) > 1 else 5
    except Exception:
        n = 5

    try:
        rows = db.fetch_recent_aggregates(pair, limit=n)
        if not rows:
            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ No hay buckets recientes para {pair}")
            return

        lines = [f"📦 Buckets recientes — {pair} (n={len(rows)})"]
        for r in rows:
            bs = r.get('bucket_start')
            avg = r.get('avg_price')
            vol = r.get('volume')
            sp = r.get('spread_pct')
            vola = r.get('volatility')
            avg_s = f"{avg:.4f}" if isinstance(avg, (int, float)) else "N/D"
            vol_s = f"{vol:.2f}" if isinstance(vol, (int, float)) else "N/D"
            sp_s = f"{sp:.2f}%" if isinstance(sp, (int, float)) else "N/D"
            vola_s = f"{vola:.4f}" if isinstance(vola, (int, float)) else "N/D"
            lines.append(f"• {bs} — avg: {avg_s} vol: {vol_s} spread: {sp_s} volat: {vola_s}")

        msg = "\n".join(lines)
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception(f"Error en /BUCKETS: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error: {e}")


# ==============================
# MAIN
# ==============================

def main():
    """Inicia el bot Telegram. If telegram lib not available, block until stopped."""
    logger.info("Iniciando bot...")
    if not TELEGRAM_AVAILABLE:
        logger.warning("python-telegram-bot no disponible. Bot deshabilitado; manteniendo proceso para worker.")
        try:
            import time
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot placeholder exiting")
        return

    # Ensure no webhook is set that would conflict with polling
    try:
        # delete_webhook is async in newer python-telegram-bot versions; run it safely
        try:
            asyncio.run(Bot(token=BOT_TOKEN).delete_webhook())
            logger.info("Deleted existing webhook (if any)")
        except Exception:
            # fallback to non-awaitable call if present
            try:
                Bot(token=BOT_TOKEN).delete_webhook()
                logger.info("Deleted existing webhook (sync fallback)")
            except Exception:
                logger.exception("Could not delete webhook (continuing)")
    except Exception:
        logger.exception("Could not delete webhook (continuing)")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    async def _log_all_updates(update, context):
        try:
            logger.debug("Received update: %s", update)
        except Exception:
            logger.exception("Error logging update")


    # Registrar handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("TASA", cmd_tasa))
    app.add_handler(CommandHandler("COP", cmd_cop))
    app.add_handler(CommandHandler("VES", cmd_ves))
    app.add_handler(CommandHandler("ARBITRAJE", cmd_arbitraje))
    app.add_handler(CommandHandler("spread", lambda u, c: _wrap_analytics(u, c, handle_spread)))
    app.add_handler(CommandHandler("merchant", lambda u, c: _wrap_analytics(u, c, handle_merchant)))
    app.add_handler(CommandHandler("volatilidad", lambda u, c: _wrap_analytics(u, c, handle_volatility)))
    app.add_handler(CommandHandler("auto_on", cmd_auto_on))
    app.add_handler(CommandHandler("auto_off", cmd_auto_off))
    app.add_handler(CommandHandler("BUCKETS", cmd_buckets))
    # Catch-all logger to help debugging when commands aren't triggering
    app.add_handler(MessageHandler(filters.ALL, _log_all_updates))

    logger.info("Bot iniciado. Escuchando comandos...")
    # Ensure an asyncio event loop is set for the main thread (fixes RuntimeError on some Python versions)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except Exception:
        pass
    app.run_polling()


async def _wrap_analytics(update, context, func):
    """Bridge between telegram handler and analytics functions.

    `func` is a synchronous function that accepts `(args: List[str], pair: str)`
    and returns a string ready to send.
    """
    chat_id = update.effective_chat.id
    args = context.args or []
    await context.bot.send_message(chat_id=chat_id, text="⏳ Procesando...")
    try:
        # allow optional pair as first arg if it looks like a PAIR (e.g. USDT-COP or USDT/COP)
        pair = 'USDT-COP'
        if args:
            first = args[0].upper()
            # heuristic: must contain '/' or '-' and also include letters to be considered a pair
            if ('/' in first or '-' in first) and any(c.isalpha() for c in first):
                pair = first
                args = args[1:]
        txt = func(args, pair)
        await context.bot.send_message(chat_id=chat_id, text=txt)
    except Exception as e:
        logger.exception(f"Error en analytics command: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error: {e}")


if __name__ == "__main__":
    main()
