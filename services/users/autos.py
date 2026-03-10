import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from core import user_db, app_config
from services.analytics.spread import handle_spread
from services.analytics.volume import handle_volume
from services.analytics.depth import handle_depth
from services.analytics.volatility import handle_volatility
from services.analytics.merchant import handle_merchant
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)

# Mapeo de comandos a funciones de análisis
HANDLERS = {
    "spread": handle_spread,
    "volume": handle_volume,
    "depth": handle_depth,
    "volatilidad": handle_volatility,
    "merchant": handle_merchant
}

_SCHEDULER = AsyncIOScheduler()

async def _run_scheduled_job(bot, user_id, command, options, task_id):
    """
    Ejecuta el comando analítico y envía el resultado al usuario.
    """
    try:
        handler = HANDLERS.get(command)
        if not handler:
            logger.error(f"Handler no encontrado para comando: {command}")
            return

        # Simular argumentos y par
        # Opciones suele ser el par_fiat (ej: 'ves') o argumentos adicionales
        args = options.split() if options else []
        
        # Obtener moneda del usuario o de las opciones
        user_currency = user_db.get_user_currency(user_id)
        pair = f"USDT-{user_currency}"
        
        # Si el primer argumento parece un par o moneda, ajustamos
        if args:
            first = args[0].upper()
            if first in ("COP", "VES", "ARS", "BRL"):
                pair = f"USDT-{first}"
                args = args[1:]

        # Ejecutar lógica de negocio original
        result_text = handler(args, pair)
        
        # Enviar mensaje al usuario
        await bot.send_message(
            chat_id=user_id,
            text=f"🕒 <b>EJECUCIÓN AUTOMÁTICA</b>\n{result_text}",
            parse_mode=ParseMode.HTML
        )
        
        # Actualizar último run en DB
        user_db.update_task_last_run(task_id)
        
    except Exception as e:
        logger.exception(f"Error ejecutando tarea automática {task_id} para {user_id}: {e}")

def start_autos(bot):
    """Inicia el planificador y carga las tareas existentes."""
    if not _SCHEDULER.running:
        _SCHEDULER.start()
        logger.info("AsyncIOScheduler para automatizaciones iniciado.")
        
    # Cargar tareas persistentes
    tasks = user_db.get_all_active_tasks()
    for t in tasks:
        add_auto_job(bot, t)
    
    logger.info(f"Se cargaron {len(tasks)} automatizaciones desde la DB.")

def add_auto_job(bot, task_dict):
    """Añade una tarea al planificador en memoria."""
    job_id = f"auto_{task_dict['id']}"
    
    # Si ya existe, lo quitamos para evitar duplicados al recargar
    if _SCHEDULER.get_job(job_id):
        _SCHEDULER.remove_job(job_id)
        
    _SCHEDULER.add_job(
        _run_scheduled_job,
        "interval",
        minutes=task_dict['interval_minutes'],
        args=[bot, task_dict['user_id'], task_dict['command'], task_dict['options'], task_dict['id']],
        id=job_id,
        replace_existing=True
    )

def remove_auto_job(task_id):
    """Quita una tarea del planificador."""
    job_id = f"auto_{task_id}"
    if _SCHEDULER.get_job(job_id):
        _SCHEDULER.remove_job(job_id)
        return True
    return False

def stop_autos():
    """Detiene el planificador."""
    if _SCHEDULER.running:
        _SCHEDULER.shutdown()
