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
    """Inicia el flujo de donación pidiendo la red primero."""
    text = (
        "☕ <b>Donar 5 USDT al Proyecto</b>\n\n"
        "Selecciona la red de tu preferencia:\n"
        "• <b>TRC20</b> (Red Tron)\n"
        "• <b>BEP20</b> (Red BSC)\n"
        "• <b>Polygon</b> (Red Polygon)\n\n"
        "⚠️ <i>Solo envía USDT a través de estas redes.</i>"
    )
    
    keyboard = [
        [InlineKeyboardButton("TRC20 (Tron)", callback_data="d_net_TRON")],
        [InlineKeyboardButton("BEP20 (BSC) ⚠️ Pendiente", callback_data="d_net_BSC")],
        [InlineKeyboardButton("Polygon", callback_data="d_net_POLYGON")],
    ]
    
    # Si viene de un callback (botón), editamos. Si es comando, enviamos nuevo.
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    else:
        await update.message.reply_html(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_callback_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los botones del menú de planes y selección de redes."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(update.effective_user.id)
    name = update.effective_user.first_name
    
    if query.data in ["p_standard", "p_pro"]:
        await query.edit_message_text(
            f"🚀 <b>¡Hola {name}!</b>\n\n"
            "Estamos trabajando duro para habilitar las suscripciones Pro.\n"
            "De momento, todos los usuarios tienen acceso casi total. "
            "Si quieres apoyar el desarrollo, puedes usar /donar.",
            parse_mode="HTML"
        )
    elif query.data == "d_5":
        # Atajo desde /planes
        await cmd_donar(update, context)
        
    elif query.data.startswith("d_net_"):
        chain = query.data.replace("d_net_", "")
        network_name = "TRC20" if chain == "TRON" else "BEP20" if chain == "BSC" else "Polygon"
        
        await query.edit_message_text(f"🔄 Generando enlace en red {network_name}...")
        
        order = ttpay.create_order(
            amount=5.0, 
            user_id=user_id, 
            chain=chain,
            description=f"Donación 5 USDT ({network_name}) - {name}"
        )
        
        if order and order.get("payment_url"):
            payment_url = order.get('payment_url')
            text = (
                f"✅ <b>Enlace Generado ({network_name})</b>\n\n"
                "Usa el botón para pagar <b>5 USDT</b>.\n"
                "• Moneda: <b>USDT</b>\n"
                "• Red: <b>{network_name}</b>\n"
                "• El enlace expira en 60 minutos."
            )
            keyboard = [[InlineKeyboardButton(f"🔗 Pagar 5 USDT ({network_name})", url=payment_url)]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        else:
            await query.edit_message_text("⚠️ Error al generar el pago. Prueba con otra red o inténtalo más tarde.")
