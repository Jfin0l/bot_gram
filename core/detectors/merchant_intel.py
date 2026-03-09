import sqlite3
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from core import app_config, db

logger = logging.getLogger(__name__)

def detect_merchant_intel(ram_window, pair: str, snap):
    """
    Registra el historial de los Top comerciantes y actualiza el registro.
    Se ejecuta en cada snapshot capturado.
    """
    cfg = app_config.DETECTORS.get('merchant_intelligence', {})
    if not cfg.get('enabled', True):
        return

    top_n = cfg.get('top_n_to_track', 50)
    
    # Separar por lados y ordenar
    buys = sorted([ad for ad in snap.ads if ad.side == 'buy'], key=lambda x: x.price, reverse=True)
    sells = sorted([ad for ad in snap.ads if ad.side == 'sell'], key=lambda x: x.price)
    
    conn = sqlite3.connect(db.DB_PATH)
    cur = conn.cursor()
    ts = snap.timestamp.isoformat()
    
    try:
        # Registrar Top N de cada lado
        _process_side(cur, buys[:top_n], pair, 'buy', ts)
        _process_side(cur, sells[:top_n], pair, 'sell', ts)
        
        # Pruning opcional (cada hora o similar, aquí lo hacemos simple cada N snapshots)
        # Para mantener ligereza, borramos registros de más de X días
        days = cfg.get('history_days', 7)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur.execute("DELETE FROM merchant_history WHERE timestamp < ?", (cutoff,))
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error en merchant_intel (detect): {e}")
    finally:
        conn.close()

def _process_side(cur, ads, pair, side, ts):
    # Obtener configuración de pesos
    cfg = app_config.DETECTORS.get('merchant_intelligence', {})
    w_freq = cfg.get('weight_frequency', 0.4)
    w_pers = cfg.get('weight_persistence', 0.3)
    w_rel  = cfg.get('weight_relist', 0.3)

    for i, ad in enumerate(ads):
        pos = i + 1
        # 1. Guardar en historial
        cur.execute(
            """
            INSERT INTO merchant_history (merchant_id, merchant_name, pair, side, price, position, volume, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ad.merchant_id, ad.merchant, pair, side, ad.price, pos, ad.quantity, ts)
        )
        
        # 2. Calcular Score rápido (basado en las últimas 24h)
        # Para no sobrecargar en cada inserción, podríamos hacerlo con una muestra aleatoria o solo cada X tiempo
        # Pero como son peticiones locales a SQLite e índices bien puestos, probamos directo
        score_data = _quick_calculate_score(cur, ad.merchant_id, w_freq, w_pers, w_rel)
        
        # 3. Actualizar Registro (Merchant Registry)
        cur.execute(
            """
            INSERT INTO merchant_registry (merchant_id, nickname, last_seen, automation_score, classification)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(merchant_id) DO UPDATE SET 
                nickname = excluded.nickname,
                last_seen = excluded.last_seen,
                automation_score = excluded.automation_score,
                classification = excluded.classification
            """,
            (ad.merchant_id, ad.merchant, ts, score_data['score'], score_data['classification'])
        )

def _quick_calculate_score(cur, m_id, w_f, w_p, w_v):
    # Versión optimizada para ejecución frecuente
    day_ago = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    
    cur.execute(
        "SELECT price, position FROM merchant_history WHERE merchant_id = ? AND timestamp > ? ORDER BY timestamp ASC",
        (m_id, day_ago)
    )
    history = cur.fetchall()
    if not history:
        return {'score': 0, 'classification': 'HUMANO'}
    
    changes = 0
    top3_hits = 0
    last_p = None
    for h in history:
        if last_p is not None and h[0] != last_p:
            changes += 1
        last_p = h[0]
        if h[1] <= 3:
            top3_hits += 1
            
    f_score = min(100, (changes / 40) * 100)
    p_score = (top3_hits / len(history)) * 100
    v_score = min(100, (len(history) / 48) * 100) # 48 = visto cada 30 min aprox
    
    final = (w_f * f_score) + (w_p * p_score) + (w_v * v_score)
    
    cl = "HUMANO"
    if final > 75: cl = "BOT/ALGORITMO"
    elif final > 45: cl = "ACTIVO"
    
    return {'score': round(final, 2), 'classification': cl}

def calculate_automation_score(merchant_id: str) -> Dict[str, Any]:
    """
    Calcula el Automation Score basado en la fórmula ponderada.
    """
    cfg = app_config.DETECTORS.get('merchant_intelligence', {})
    w_freq = cfg.get('weight_frequency', 0.4)
    w_pers = cfg.get('weight_persistence', 0.3)
    w_rel  = cfg.get('weight_relist', 0.3)
    
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    try:
        # 1. Frecuencia de cambios (F)
        # Cambios de precio en las últimas 24h
        day_ago = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        cur.execute(
            "SELECT price, timestamp FROM merchant_history WHERE merchant_id = ? AND timestamp > ? ORDER BY timestamp ASC",
            (merchant_id, day_ago)
        )
        rows = cur.fetchall()
        
        changes = 0
        last_price = None
        for r in rows:
            if last_price is not None and r['price'] != last_price:
                changes += 1
            last_price = r['price']
        
        # Normalización F: 0-100. Consideramos > 50 cambios/día como 100% automatizado
        f_score = min(100, (changes / 50) * 100)
        
        # 2. Persistencia en Top (P)
        # % de apariciones en Top 3 en el historial disponible (últimas 24h)
        cur.execute(
            "SELECT COUNT(*) FROM merchant_history WHERE merchant_id = ? AND timestamp > ? AND position <= 3",
            (merchant_id, day_ago)
        )
        top3_count = cur.fetchone()[0]
        total_snaps = len(rows) if rows else 1
        p_score = (top3_count / total_snaps) * 100
        
        # 3. Velocidad de Relist (V)
        # (Simplificado: tiempo promedio entre snaps visto que estuvo ausente)
        # Por ahora usaremos un proxy: Variabilidad de la posición. Bots suelen estar fijos en Top 1-2.
        # Mejoramos a: % de tiempo activo.
        v_score = min(100, (total_snaps / 40) * 100) # Proxy de actividad intensa
        
        final_score = (w_freq * f_score) + (w_pers * p_score) + (w_rel * v_score)
        
        # Clasificación
        if final_score > 70:
            status = "BOT/ALGORITMO"
        elif final_score > 40:
            status = "ACTIVO"
        else:
            status = "HUMANO"
            
        return {
            'score': round(final_score, 2),
            'classification': status,
            'metrics': {
                'changes_24h': changes,
                'persistence_top3_pct': round(p_score, 2),
                'active_snaps_24h': total_snaps
            }
        }
        
    except Exception as e:
        logger.error(f"Error calculando score para {merchant_id}: {e}")
        return {'score': 0, 'classification': 'ERROR', 'metrics': {}}
    finally:
        conn.close()
