import time
import logging
import asyncio
from functools import wraps
from telegram import Update
try:
    from telegram.ext import ContextTypes
except Exception:
    ContextTypes = None

from core.user_db import check_daily_limits, log_usage, get_next_in_waitlist

logger = logging.getLogger(__name__)


async def try_promote_next_waitlist(context):
    next_user = get_next_in_waitlist()
    if next_user:
        try:
            await context.bot.send_message(
                chat_id=int(next_user),
                text="🔔 <b>¡Un asiento se ha liberado en FASTMONEY Bot!</b>\n\nTu turno ha llegado. Ahora tienes un slot activo con acceso a 15 consultas. ¡Aprovecha el mercado!",
                parse_mode="HTML"
            )
            logger.info(f"Usuario {next_user} promovido a slot activo.")
        except Exception as e:
            logger.error(
                f"Error notificando waitlist al user {next_user}: {e}")


def rate_limited(command_name: str):
    """
    Decorador para comandos de Telegram que aplica control de cuotas y loguea la respuesta.
    Maneja asientos dinamicos, lista negra y colas de espera.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context, *args, **kwargs):
            if not update or not update.effective_user:
                return await func(update, context, *args, **kwargs)

            user_id = str(update.effective_user.id)
            chat_id = update.effective_chat.id
            start_time = time.perf_counter()
            
            # Obtener exchange actual del usuario para métricas
            from core.user_db import get_user_exchange
            current_exch = get_user_exchange(user_id)
            usage_details = {"exchange": current_exch}

            # Chequear límites y slots dinámicos
            can_proceed, reason = check_daily_limits(user_id)

            if not can_proceed:
                response_time = time.perf_counter() - start_time
                if reason == "BANNED":
                    # Ignorado silenciosamente
                    log_usage(user_id, command_name, 'BANNED', response_time)
                    return
                elif reason == "USER_LIMIT_REACHED":
                    msg = "⚠️ <b>Límite Diario Alcanzado</b>\n\nHas llegado al máximo de 15 consultas por día. ¡Vuelve mañana!\n\n<i>Tu sesión ha terminado y tu asiento se ha liberado para otro trader.</i>"
                    result_status = 'LIMIT_USER'
                elif reason.startswith("WAITLIST_"):
                    pos = reason.split("_")[1]
                    msg = f"🚧 <b>Capacidad Máxima (Cupo Lleno)</b>\n\nEl bot está operando a tope con 30 traders simultáneos de forma activa.\n\nHas sido añadido a la lista de espera automática (Posición: <b>#{pos}</b>).\n\n<i>Te avisaremos cuando alguien termine sus consultas y tengas un asiento.</i>"
                    result_status = 'WAITLIST'
                elif reason.startswith("ALREADY_WAITLIST_"):
                    pos = reason.split("_")[2]
                    msg = f"⏳ Sigues en la cola de espera (Posición: <b>#{pos}</b>)."
                    result_status = 'ALREADY_WAITLIST'
                else:
                    msg = "⚠️ No puedes procesar más consultas por el momento."
                    result_status = 'ERROR'

                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=msg,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(
                        f"Error enviando mensaje limit/waitlist a {chat_id}: {e}")

                log_usage(user_id, command_name, result_status, response_time, details=usage_details)
                return

            # Si el usuario puede proceder, ejecutamos la función original
            result_status = 'SUCCESS'
            error_thrown = False
            try:
                await func(update, context, *args, **kwargs)
            except Exception as e:
                result_status = 'ERROR'
                error_thrown = True
                raise e  # Throw it so the standard error handler catches it
            finally:
                res_time = time.perf_counter() - start_time
                log_usage(user_id, command_name, result_status, res_time, details=usage_details)

                # Checkear si acaba de consumir su solicitud 15 exactament!
                import sqlite3
                from core.user_db import DB_PATH
                from datetime import datetime, timezone
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cur = conn.cursor()
                    today = datetime.now(timezone.utc).isoformat()[:10]
                    cur.execute(
                        "SELECT COUNT(*) FROM bot_usage_logs WHERE user_id = ? AND timestamp LIKE ? AND result NOT IN ('LIMIT_USER', 'CAPACITY_FULL', 'WAITLIST', 'BANNED', 'ALREADY_WAITLIST')", (str(user_id), f"{today}%"))
                    reqs_now = cur.fetchone()[0]
                    conn.close()

                    if reqs_now == 15 and not error_thrown:
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text="ℹ️ <b>Mensaje del Sistema:</b>\nEsta ha sido tu consulta número 15. Tu saldo para hoy se agotó y tu asiento será cedido al siguiente trader en espera.\n\n¡Te esperamos de vuelta mañana!",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                        # Disparar background task para notificar a la cola
                        if getattr(context, 'application', None):
                            # Si es un loop de main normal
                            asyncio.create_task(
                                try_promote_next_waitlist(context))
                except Exception as e:
                    logger.error(
                        f"Error verificando liberación de slot post-comando: {e}")

        return wrapper
    return decorator
