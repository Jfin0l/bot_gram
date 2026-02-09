"""
Telegram Bot entrypoint — Integración completa con nueva arquitectura DB.
Comandos: /TASA, /COP, /VES, /ARBITRAJE, /auto_on, /auto_off, /start
"""
import logging
import os
from dotenv import load_dotenv
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode
from datetime import datetime

from core import pipeline, notifier, db, scheduler

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OWNER_ID = os.getenv("OWNER_ID")

if not BOT_TOKEN or not CHAT_ID:
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


# ==============================
# HANDLERS
# ==============================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cmd_tasa(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cmd_cop(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cmd_ves(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cmd_arbitraje(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cmd_auto_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cmd_auto_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


# ==============================
# MAIN
# ==============================

def main():
    """Inicia el bot Telegram."""
    logger.info("Iniciando bot...")
    
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Registrar handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("TASA", cmd_tasa))
    app.add_handler(CommandHandler("COP", cmd_cop))
    app.add_handler(CommandHandler("VES", cmd_ves))
    app.add_handler(CommandHandler("ARBITRAJE", cmd_arbitraje))
    app.add_handler(CommandHandler("auto_on", cmd_auto_on))
    app.add_handler(CommandHandler("auto_off", cmd_auto_off))
    
    logger.info("Bot iniciado. Escuchando comandos...")
    app.run_polling()


if __name__ == "__main__":
    main()
