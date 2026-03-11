import sqlite3
import logging
from pathlib import Path
from core.db import init_db, init_merchant_stats_table
from core.user_db import init_user_db, DB_PATH as USER_DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def check_migrations():
    """Realiza verificaciones de integridad y migraciones pendientes."""
    logger.info("Verificando integridad de base de datos...")
    
    # Asegurar que las tablas base existen
    init_db()
    init_user_db()
    init_merchant_stats_table()
    
    # Migración: Verificar columna 'tier' en user_preferences
    conn = sqlite3.connect(USER_DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(user_preferences)")
        columns = [column[1] for column in cur.fetchall()]
        if 'tier' not in columns:
            logger.info("Migración: Añadiendo columna 'tier' a user_preferences...")
            cur.execute("ALTER TABLE user_preferences ADD COLUMN tier TEXT DEFAULT 'FREE'")
            conn.commit()
            logger.info("Columna 'tier' añadida exitosamente.")
        else:
            logger.debug("Columna 'tier' ya existe.")
    except Exception as e:
        logger.error(f"Error en migración de 'tier': {e}")
    finally:
        conn.close()

    logger.info("Base de datos lista para operar.")

if __name__ == "__main__":
    check_migrations()
