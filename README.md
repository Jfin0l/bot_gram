# FastMoney Bot P2P — Bot Telegram Funcional

Proyecto de análisis de mercados P2P conectado a Binance P2P con mensajes automáticos a Telegram.

## ✅ Status Actual

**Prototipo funcional:** El bot está listo para enviar mensajes automáticos (`/auto_on`, `/auto_off`) y responder a comandos.

## Inicio Rápido

### 1. Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar `.env`

Copia de `.env.example`:
```bash
cp .env.example .env
```

Edit `.env` con tu token y chat ID (ya tiene valores de ejemplo, solo ajusta si es necesario).

### 3. Ejecutar el Bot

```bash
# Ejecutar el bot (escucha comandos de Telegram)
python3 -m scripts.run_bot
```

El bot está vivo cuando ves:
```
INFO | services.telegram_bot_main | Bot iniciado. Escuchando comandos...
```

## Comandos Disponibles

| Comando | Descripción | Ejemplo |
|---------|-------------|---------|
| `/start` | Muestra ayuda y comandos | `/start` |
| `/TASA` | Tasas actuales (COP↔VES) | `/TASA` |
| `/COP` | Mercado USDT-COP compacto | `/COP` |
| `/VES` | Mercado USDT-VES compacto | `/VES` |
| `/ARBITRAJE` | Análisis de oportunidades | `/ARBITRAJE` |
| `/auto_on [minutos]` | Envíos automáticos cada N minutos (default 60) | `/auto_on 30` |
| `/auto_off` | Desactiva envíos automáticos | `/auto_off` |

**Nota:** `/auto_on` y `/auto_off` solo funcionan si ejecutas desde el ID en `OWNER_ID` en `.env`.

## Arquitectura (Backend sin UI)

```
Binance API
    ↓
fetcher (1 request por mercado)
    ↓
DB SQLite (raw_responses, snapshots)
    ↓
pipeline (análisis desde raw)
    ↓
notifier (formateo de mensajes)
    ↓
Telegram Bot (handlers de comandos + auto-schedule)
```

## Workers en Segundo Plano (Opcionales)

Si quieres que los datos se actualicen automáticamente sin el bot ejecutándose:

```bash
# En otra terminal: Actualizar datos cada 10 min
python3 -m scripts.run_worker

# En otra terminal: Notificador (envía tasas cada 1 hora, dry-run)
python3 -m scripts.run_notifier
```

## Base de Datos

Ubicación: `data/p2p_data.db` (SQLite)

**Tablas:**
- `raw_responses` — respuestas crudas de la API Binance
- `snapshots` — resúmenes agregados (precios, volatilidad, arbitraje)

Cada comando Telegram lee desde estas tablas.

## Migraciones Realizadas

✅ CSV → SQLite  
✅ Arquitectura monolítica → Modular (core/db, fetcher, pipeline, notifier)  
✅ Bot integrado con DB  
✅ Envíos automáticos con scheduler  

## Próximos Pasos (Futuro)

- [ ] API REST (FastAPI) para acceso HTTP
- [ ] Gestión de usuarios (Free/Premium)
- [ ] Soporte para otros exchanges
- [ ] Gráficas históricas
- [ ] Alertas por volatilidad
- [ ] Systemd services para auto-start en boot

## Debugging

### ¿El bot no responde?

```bash
# Verifica que hay datos en DB
python3 - <<'PY'
from core.db import fetch_latest_snapshots
print("Snapshots en DB:", len(fetch_latest_snapshots(3)))
PY

# Obtén datos frescos
python3 -m scripts.fetch_and_store

# Genera snapshots
python3 -m scripts.generate_snapshot
```

### ¿Error de BOT_TOKEN?

```bash
# Verifica .env
cat .env | grep BOT_TOKEN

# Debe ser: BOT_TOKEN=<tu_token_aqui> (sin espacios)
```

## Archivos Clave

- `services/telegram_bot_main.py` — Bot Telegram (handlers + scheduler)
- `scripts/run_bot.py` — Runner para ejecutar el bot
- `core/pipeline.py` — Análisis desde DB
- `core/notifier.py` — Formateo de mensajes
- `core/db.py` — Helpers SQLite

## Status

**Prototipo Base Funcional ✅** — Feb 2026

Ver [MIGRACION_RESUMEN.md](MIGRACION_RESUMEN.md) para arquitectura detallada.
