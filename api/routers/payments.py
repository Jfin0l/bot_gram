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
    
    # 2. Procesar los datos (según estructura de TTPay)
    # Ejemplo esperado: {"status": "success", "out_trade_no": "...", "transaction_id": "..."}
    status = data.get("status")
    out_trade_no = data.get("out_trade_no")
    transaction_id = data.get("transaction_id")
    
    if status == "success" or status == "completed":
        logger.info(f"¡Pago exitoso confirmado! Orden: {out_trade_no}")
        db.update_donation_status(out_trade_no, "COMPLETED", transaction_id)
        # Aquí podrías notificar al bot de Telegram si quisieras una alerta inmediata
    else:
        logger.warning(f"Pago fallido o pendiente: {status} para {out_trade_no}")
        db.update_donation_status(out_trade_no, status, transaction_id)

    return {"code": 0, "msg": "ok"}
