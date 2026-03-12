from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class TTPayWebhookPayload(BaseModel):
    """Esquema de validación para las notificaciones de TTPay."""
    original_type: str = Field(..., description="Tipo de objeto antes del cifrado (ej: transaction)")
    algorithm: str = Field(..., description="Algoritmo de cifrado (ej: AEAD_AES_256_GCM)")
    ciphertext: str = Field(..., description="Datos cifrados en Base64")
    nonce: str = Field(..., description="Iv/Nonce utilizado para el cifrado")

class DonationResponse(BaseModel):
    """Respuesta estándar para consultas de donación."""
    status: str
    out_trade_no: str
    message: Optional[str] = None
