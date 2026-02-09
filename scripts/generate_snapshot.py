
from core.snapshot import create_and_store_snapshots

# Config minimal (evita importar `p2p_info` y sus dependencias pesadas)
CONFIG = {
    "monedas": {"COP": {"rows": 20, "page": 2}, "VES": {"rows": 20, "page": 4}},
    "filas_tasa_remesa": 5,
    "ponderacion_volumen": True,
    "limite_outlier": 0.025,
}


def main():
    create_and_store_snapshots(CONFIG)


if __name__ == "__main__":
    main()
