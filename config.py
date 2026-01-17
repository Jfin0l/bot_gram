# config.py
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

# Cargar variables del entorno (.env)
load_dotenv()

# Variables globales
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OWNER_ID = os.getenv("OWNER_ID")
PRIVATE_ID = os.getenv("PRIVATE_ID")  # opcional, por seguridad
DATA_PATH = os.path.join(os.getcwd(), "data")
SNAPSHOT_PATH = os.path.join(DATA_PATH, "snapshots")
ADS_PATH = os.path.join(DATA_PATH, "ads")

# Formateo numérico uniforme
def fmt(num, dec=1):
    try:
        return f"{float(num):.{dec}f}"
    except (TypeError, ValueError):
        return num

# Logger simple
def get_logger(name="bot"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

log = get_logger()
