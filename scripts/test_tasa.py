"""Script de prueba para el comando /TASA.
Muestra en consola el mensaje que enviaría el bot y permite enviar en `dry_run`.
"""
import argparse
from core import pipeline, notifier
from core import db


def main(dry_run=True):
    config = {
        "pares": ["USDT-COP", "USDT-VES"],
        "monedas": {"COP": {"rows": 20, "page": 2}, "VES": {"rows": 20, "page": 4}},
        "filas_tasa_remesa": 5,
        "ponderacion_volumen": True,
        "limite_outlier": 0.025,
        "umbral_volatilidad": 3,
    }

    data = pipeline.build_data_from_db(config)
    msg = notifier.format_tasa(data)

    print("--- Mensaje /TASA (preview) ---")
    print(msg)
    print("--------------------------------")

    # Intentar enviar (dry_run imprimirá en logs)
    ok = notifier.send_message(chat_id=None, text=msg, parse_mode="HTML", dry_run=dry_run)
    print("Envío (dry_run=%s): %s" % (dry_run, ok))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="Enviar en modo live (no dry-run)")
    args = ap.parse_args()
    main(dry_run=not args.live)
