#!/usr/bin/env python3
from core.notifier import start_notifier_scheduler

# Evitar importar p2p_info para no cargar dependencias pesadas; usar
# CONFIG local por defecto. En despliegue puedes pasar la CONFIG del
# archivo principal.
CONFIG = {
    "monedas": {"COP": {"rows": 20, "page": 2}, "VES": {"rows": 20, "page": 4}},
    "filas_tasa_remesa": 5,
    "ponderacion_volumen": True,
    "limite_outlier": 0.025,
}


def main():
    # dry_run=True evita enviar mensajes reales si no se configuró BOT_TOKEN
    sched = start_notifier_scheduler(CONFIG, interval=3600, dry_run=True)
    try:
        import time
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()


if __name__ == '__main__':
    main()
