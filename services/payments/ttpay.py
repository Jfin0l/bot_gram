import os
import time
import json
import base64
import hashlib
import requests
import logging
from Crypto.Cipher import AES
from Crypto.Util import Padding
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class TTPayService:
    def __init__(self):
        self.app_id = os.getenv("TTPAY_APP_ID")
        self.mch_id = os.getenv("TTPAY_MCH_ID")
        self.secret_key = os.getenv("TTPAY_SECRET_KEY")
        self.api_url = "https://api.tokenpay.me/v1/transaction/prepayment"
        
        if not all([self.app_id, self.mch_id, self.secret_key]):
             logger.warning("Credenciales de TTPay incompletas en el archivo .env")

    def _generate_nonce(self) -> str:
        """Genera un nonce de 32 bits aleatorio."""
        return hashlib.md5(str(time.time()).encode()).hexdigest().upper()

    def _encrypt_signature(self, signature_str: str) -> str:
        """
        Cifra la cadena de firma usando AES-256-ECB + PKCS7.
        Algoritmo: TTPAY-AES-256-ECB
        """
        key = self.secret_key.encode('utf-8')
        cipher = AES.new(key, AES.MODE_ECB)
        # TTPay usa PKCS7 padding
        padded_data = Padding.pad(signature_str.encode('utf-8'), AES.block_size)
        encrypted = cipher.encrypt(padded_data)
        return base64.b64encode(encrypted).decode('utf-8')

    def create_order(self, amount: float, user_id: str, description: str = "Donación FastMoney Bot") -> Optional[Dict[str, Any]]:
        """
        Crea una orden en TTPay y devuelve la URL de pago.
        """
        nonce = self._generate_nonce()
        # Probar con segundos (10 dígitos) como en el ejemplo
        timestamp = str(int(time.time()))
        out_trade_no = f"PAY-{user_id}-{int(time.time())}"
        
        # Cuerpo del mensaje
        body = {
            "app_id": self.app_id,
            "mch_id": self.mch_id,
            "description": description,
            "out_trade_no": out_trade_no,
            "expire_second": 3600,
            "amount": amount,
            "chain": "TRON",
            "currency": "USDT",
            "to_address": "",
            "attach": str(user_id),
            "locale": "en",
            "notify_url": os.getenv("WEBHOOK_URL", ""),
            "return_url": "",
            "order_type": "platform_order"
        }
        
        # Construir string de firma: URL\nTimestamp\nNonce\nBody
        path = "/v1/transaction/prepayment"
        body_json = json.dumps(body, separators=(',', ':'))
        signature_base = f"{path}\n{timestamp}\n{nonce}\n{body_json}"
        
        logger.debug(f"Signature Base String: [{signature_base.replace('\n', '\\n')}]")
        
        signature = self._encrypt_signature(signature_base)
        
        # Header de Autorización sin espacios tras comas
        auth_header = (
            f"TTPAY-AES-256-ECB app_id={self.app_id},"
            f"mch_id={self.mch_id},nonce_str={nonce},"
            f"timestamp={timestamp},signature={signature}"
        )
        
        headers = {
            "Authorization": auth_header,
            "User-Agent": "tokenpay API (https://tokenpay.me)",
            "Content-Type": "application/json"
        }
        
        try:
            logger.info(f"Enviando solicitud a TTPay ({self.api_url})...")
            # Enviamos body_json directamente para asegurar coincidencia total
            response = requests.post(self.api_url, data=body_json, headers=headers, timeout=10)
            res_data = response.json()
            
            logger.info(f"Respuesta de TTPay: {json.dumps(res_data)}")
            
            if res_data.get("code") == 0:
                # Éxito: Guardamos en nuestra DB local como pendiente
                from core import db
                db.save_donation(user_id, amount, out_trade_no)
                return res_data.get("data")
            else:
                 logger.error(f"Error TTPay (Code {res_data.get('code')}): {res_data.get('msg')}")
                 return None
        except Exception as e:
            logger.error(f"Fallo en create_order: {e}")
            return None

    def decrypt_webhook(self, ciphertext: str, nonce: str) -> Optional[Dict[str, Any]]:
        """
        Descifra el payload del webhook usando AES-256-GCM.
        """
        try:
            key = self.secret_key.encode('utf-8')
            cipher_data = base64.b64decode(ciphertext)
            nonce_bytes = nonce.encode('utf-8')
            
            # En GCM, los últimos 16 bytes suelen ser el tag de autenticación
            tag = cipher_data[-16:]
            actual_ciphertext = cipher_data[:-16]
            
            cipher = AES.new(key, AES.MODE_GCM, nonce=nonce_bytes)
            decrypted_data = cipher.decrypt_and_verify(actual_ciphertext, tag)
            
            return json.loads(decrypted_data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Error descifrando webhook: {e}")
            return None
