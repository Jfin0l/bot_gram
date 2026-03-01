import sqlite3
from datetime import datetime
from core.db import DB_PATH

def store_hourly_merchant_stats(pair: str):
    """Guarda estadísticas de la última hora para todos los merchants."""
    from core.ram_window import get_global
    
    rw = get_global()
    if not rw:
        return
    
    now = datetime.utcnow()
    current_hour = now.hour
    today_str = now.strftime("%Y-%m-%d")
    
    cutoff = now.timestamp() - 3600
    
    stats = {}
    
    with rw.lock:
        for merchant, ads_list in rw.merchant_index.items():
            for ts, ad in ads_list:
                if ts.timestamp() < cutoff:
                    continue
                
                key = (merchant, ad.side)
                if key not in stats:
                    stats[key] = {'vol': 0.0, 'sum_price': 0.0, 'count': 0}
                
                usdt_vol = ad.quantity / ad.price if ad.price > 0 else 0
                stats[key]['vol'] += usdt_vol
                stats[key]['sum_price'] += ad.price
                stats[key]['count'] += 1
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for (merchant, side), data in stats.items():
        avg_price = data['sum_price'] / data['count'] if data['count'] > 0 else 0
        c.execute("""
            INSERT OR REPLACE INTO merchant_stats
            (merchant, pair, side, volume_usdt, avg_price, ad_count, hour, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            merchant, pair, side, data['vol'], avg_price,
            data['count'], current_hour, today_str
        ))
    
    conn.commit()
    conn.close()