from core.notifier import send_tasa_to_channel
from core import db
from dotenv import load_dotenv
import os

load_dotenv()

# Reads BOT_TOKEN and CHAT_ID from .env via config.py
# If they are missing, notifier will do a dry-run and print the message instead of sending.

# Minimal CONFIG for pipeline
CONFIG = {
    "monedas": {"COP": {"rows": 20, "page": 2}, "VES": {"rows": 20, "page": 4}},
    "filas_tasa_remesa": 5,
    "ponderacion_volumen": True,
    "limite_outlier": 0.025,
}

if __name__ == '__main__':
    # If you want to send to a different chat, set TEST_CHAT_ID env var
    target = os.getenv('TEST_CHAT_ID')
    ok = send_tasa_to_channel(CONFIG, chat_id=target, dry_run=False)
    print('Send OK:', ok)
