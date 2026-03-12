from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.payments.ttpay import TTPayService
from core.processor import ai_meta
import os

ttpay = TTPayService()

async def cmd_planes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el menú de planes disponibles."""
    text = (
        "💎 <b>NUESTROS PLANES</b>\n\n"
        "Actualmente estamos en fase Alpha pública. Los planes Pro se activarán próximamente:\n\n"
        "1. <b>Free</b>: Métricas básicas y /spread limitado.\n"
        "2. <b>Standard (Pronto)</b>: Acceso a /merchant y alertas 24/7.\n"
        "3. <b>Pro (Pronto)</b>: Exportación XLS y filtros avanzados por banco.\n\n"
        "💡 Si valoras nuestro trabajo, puedes apoyarnos con una donación para mantener el servidor online."
    )
    
    keyboard = [
        [InlineKeyboardButton("💳 Quieres ser Standard?", callback_data="p_standard")],
        [InlineKeyboardButton("🔥 Quiero ser PRO", callback_data="p_pro")],
        [InlineKeyboardButton("☕ Donar un café (5 USDT)", callback_data="d_5")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_html(text, reply_markup=reply_markup)

async def cmd_donar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inicia el flujo de donación rápida."""
    # Por defecto sugerimos una donación de 5 USDT
    user_id = str(update.effective_user.id)
    name = update.effective_user.first_name
    
    await update.message.reply_text("🔄 Generando tu enlace de pago seguro via TTPay...")
    
    order = ttpay.create_order(amount=5.0, user_id=user_id, description=f"Café para el equipo - {name}")
    
    if order and order.get("payment_url"):
        payment_url = f"https://ttpay.io{order.get('payment_url')}"
        
        text = (
            f"✅ <b>¡Gracias {name}!</b>\n\n"
            "Haz clic en el botón de abajo para completar tu donación de <b>5 USDT</b>.\n"
            "• Red: TRON (USDC/USDT)\n"
            "• El enlace expira en 60 minutos."
        )
        keyboard = [[InlineKeyboardButton("🔗 Ir a Pagar (TTPay)", url=payment_url)]]
        await update.message.reply_html(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("⚠️ No se pudo generar el enlace en este momento. Inténtalo más tarde.")

async def handle_callback_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los botones del menú de planes."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(update.effective_user.id)
    name = update.effective_user.first_name
    
    if query.data in ["p_standard", "p_pro"]:
        await query.edit_message_text(
            f"🚀 <b>¡Hola {name}!</b>\n\n"
            "Estamos trabajando duro para habilitar las suscripciones Pro.\n"
            "De momento, todos los usuarios tienen acceso casi total. "
            "Si quieres apoyar el desarrollo, usa /donar.",
            parse_mode="HTML"
        )
    elif query.data == "d_5":
        order = ttpay.create_order(amount=5.0, user_id=user_id, description=f"Café via botón - {name}")
        if order and order.get("payment_url"):
             payment_url = f"https://ttpay.io{order.get('payment_url')}"
             await query.edit_message_text(
                 f"☕ <b>Donar 5 USDT</b>\n\nPresiona el botón para abrir la pasarela:",
                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Pagar 5 USDT", url=payment_url)]]),
                 parse_mode="HTML"
             )
