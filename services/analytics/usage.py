import sqlite3
from datetime import datetime, timedelta, timezone
from core.user_db import DB_PATH


def generate_cso_report() -> str:
    """Genera el reporte semanal de análisis de usuarios para el CSO."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Análisis de los últimos 7 días
    now = datetime.now(timezone.utc)
    week_ago = (now - timedelta(days=7)).isoformat()

    try:
        # 1. Usuarios recurrentes (Retention) - >3 días en la semana
        cur.execute('''
            SELECT user_id, COUNT(DISTINCT substr(timestamp, 1, 10)) as days_active
            FROM bot_usage_logs
            WHERE timestamp >= ?
            GROUP BY user_id
        ''', (week_ago,))
        retention_rows = cur.fetchall()

        total_users_week = len(retention_rows)
        traders_reales = sum(1 for row in retention_rows if row[1] >= 3)
        curiosos = total_users_week - traders_reales

        # 2. Power users (Agotaron 15 solicitudes en 1 día)
        cur.execute('''
            SELECT user_id, substr(timestamp, 1, 10) as day, COUNT(*) as reqs
            FROM bot_usage_logs
            WHERE timestamp >= ? AND result != 'CAPACITY_FULL'
            GROUP BY user_id, day
            HAVING reqs >= 15
        ''', (week_ago,))
        power_user_rows = cur.fetchall()
        power_users_count = len(set([row[0] for row in power_user_rows]))

        # 3. Análisis de Escasez (Días con 30 usuarios)
        cur.execute('''
            SELECT substr(timestamp, 1, 10) as day, COUNT(DISTINCT user_id) as users_count, MIN(substr(timestamp, 12, 5)) as time_full
            FROM bot_usage_logs
            WHERE timestamp >= ?
            GROUP BY day
            HAVING users_count >= 30
        ''', (week_ago,))
        scarcity_rows = cur.fetchall()
        dias_llenos = len(scarcity_rows)
        horas_lleno = [row[2] for row in scarcity_rows]
        hora_promedio_lleno = min(horas_lleno) if horas_lleno else "N/A"

        # 4. Psicología / Comandos
        cur.execute('''
            SELECT command, COUNT(*) as count
            FROM bot_usage_logs
            WHERE timestamp >= ?
            GROUP BY command
            ORDER BY count DESC
            LIMIT 3
        ''', (week_ago,))
        top_commands = cur.fetchall()
        dolor_usuario = "Desconocido"
        if top_commands:
            cmd = top_commands[0][0]
            if cmd in ['/TASA', '/COP', '/VES']:
                dolor_usuario = "Saber el precio rápido"
            elif cmd in ['/ARBITRAJE', '/spread']:
                dolor_usuario = "Calcular la comisión/spread"
            elif cmd in ['/merchant', '/volatilidad']:
                dolor_usuario = "Buscar seguridad en los vendedores"

        # 5. Detección de Errores (cuantos usuarios causan errores seguido)
        cur.execute('''
            SELECT COUNT(*) 
            FROM bot_usage_logs
            WHERE timestamp >= ? AND result = 'ERROR'
        ''', (week_ago,))
        errores_count = cur.fetchone()[0]

        # 6. Tiempo de sesión (Traders vs Monitors)
        cur.execute(
            '''SELECT user_id, timestamp FROM bot_usage_logs WHERE timestamp >= ? ORDER BY user_id, timestamp''', (week_ago,))
        logs = cur.fetchall()

        trader_profiles = set()
        from collections import defaultdict
        user_times = defaultdict(list)
        for uid, ts in logs:
            try:
                user_times[uid].append(datetime.fromisoformat(ts).timestamp())
            except:
                pass

        for uid, times in user_times.items():
            # Traders ejecutando operaciones en vivo (Ej: 5 a 10 consultas en 5 minutos)
            if len(times) >= 5:
                # buscar ventanas deslizantes pequeñas
                is_trader = False
                for i in range(len(times) - 4):
                    if times[i+4] - times[i] <= 300:  # 5 consultas en <= 5 mins
                        trader_profiles.add(uid)
                        is_trader = True
                        break

        monitor_profiles = set(user_times.keys()) - trader_profiles

    except Exception as e:
        return f"⚠️ Error generando reporte CSO: {e}"
    finally:
        conn.close()

    # Lógica Semáforo
    if dias_llenos >= 4 or power_users_count >= (total_users_week * 0.2 if total_users_week else 0):
        semaforo = "🟢 VERDE (Alta demanda y retención. Listo para escalar a 100 usuarios/monetizar)"
    elif total_users_week >= 15 or traders_reales > 5:
        semaforo = "🟡 AMARILLO (Tracción estable, optimizar engagement antes de subir cupos)"
    else:
        semaforo = "🔴 ROJO (Aún en fase de discovery, demanda baja)"

    # Formateo de cadena
    lines = [
        "📊 <b>REPORTE EJECUTIVO CSO</b> 📊",
        "",
        f"<b>1. Tránsito Semanal ({total_users_week} usuarios únicos)</b>",
        f"• Retención: {traders_reales} Traders Reales vs {curiosos} Curiosos",
        f"• Perfiles: {len(trader_profiles)} Alta Velocidad (Traders) | {len(monitor_profiles)} Monitores",
        "",
        "<b>2. Análisis de Escasez y Poder</b>",
        f"• Power Users (Agotan 15 reqs): {power_users_count} usuarios",
        f"• Cupo lleno (30 max): {dias_llenos} de los últimos 7 días",
    ]
    if dias_llenos > 0:
        lines.append(
            f"  └ <i>Métrica Estrella:</i> El cupo se llenó en promedio a las {hora_promedio_lleno} UTC.")

    lines.extend([
        "",
        "<b>3. Psicología y Fricción</b>",
        f"• Dolor Principal Identificado: <b>{dolor_usuario}</b>",
        f"• Comandos Top: {', '.join([c[0] for c in top_commands])}",
        f"• Fricción: {errores_count} errores analizados",
        "",
        "<b>4. Roadmap y Viabilidad</b>",
        f"🚥 {semaforo}",
        ""
    ])

    # Feature suggestions based on behaviour
    lines.append("💡 <b>Sugerencia de 'Feature':</b>")
    if "Saber el precio" in dolor_usuario:
        lines.append(
            "El usuario sufre por velocidad. Agregar *modo Alertas Automáticas* para ahorrarles llamadas manuales.")
    elif "Calcular la comisión/spread" in dolor_usuario:
        lines.append(
            "El usuario calcula rentabilidad. Agregar *función `/calculadora <monto>`* para mostrar ganancia neta restando fees de Binance y envíos.")
    else:
        lines.append(
            "El usuario teme estafas. Potenciar *score de comerciantes* con `/merchant info <id>` para validar buena fe.")

    return "\n".join(lines)
