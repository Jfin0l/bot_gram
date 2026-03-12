from fastapi import APIRouter, Header, Request, HTTPException
from api.schemas import TTPayWebhookPayload
from services.payments.ttpay import TTPayService
from core import db
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
ttpay = TTPayService()

@router.post("/webhook-ttpay")
async def ttpay_webhook(payload: TTPayWebhookPayload):
    """
    Recibe notificaciones de pago de TTPay.
    """
    logger.info(f"Recibiendo webhook de TTPay: {payload.algorithm}")
    
    # 1. Descifrar el mensaje
    data = ttpay.decrypt_webhook(payload.ciphertext, payload.nonce)
    
    if not data:
        raise HTTPException(status_code=400, detail="Invalid signature or decryption failed")
    
    # 2. Procesar los datos
    status = data.get("status")
    out_trade_no = data.get("out_trade_no")
    transaction_id = data.get("transaction_id")
    
    if status in ["success", "completed"]:
        logger.info(f"¡Pago exitoso confirmado! Orden: {out_trade_no}")
        db.update_donation_status(out_trade_no, "COMPLETED", transaction_id)
        
        # Notificar al usuario vía Telegram
        donation = db.get_donation_by_trade_no(out_trade_no)
        if donation:
            import requests
            import os
            token = os.getenv("BOT_TOKEN")
            user_id = donation["user_id"]
            msg = (
                "✅ <b>¡Donación Confirmada!</b>\n\n"
                "Hemos recibido tu pago de 5 USDT. ¡Muchísimas gracias por apoyar el proyecto! ☕\n\n"
                f"ID Transacción: <code>{transaction_id}</code>"
            )
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            try:
                requests.post(url, json={"chat_id": user_id, "text": msg, "parse_mode": "HTML"})
            except Exception as e:
                logger.error(f"Error enviando notificación al bot: {e}")
    else:
        logger.warning(f"Pago fallido o pendiente: {status} para {out_trade_no}")
        db.update_donation_status(out_trade_no, status, transaction_id)

    return {"code": 0, "msg": "ok"}
