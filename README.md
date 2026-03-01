# FastMoney Bot P2P — Bot Telegram de Analisis P2P

Bot de Telegram para analisis del mercado P2P de Binance, enfocado en los pares **USDT-COP** y **USDT-VES**. Calcula tasas de cambio, spreads, arbitraje, volatilidad y actividad de merchants.

## Status Actual

**Prototipo funcional** con dos pipelines de datos:
- **DB-centric (legacy):** Para comandos base (`/TASA`, `/COP`, `/VES`, `/ARBITRAJE`)
- **RAM in-memory (nuevo):** Para analytics avanzados (`/spread`, `/merchant`, `/volatilidad`, `/BUCKETS`)

## Inicio Rapido

### 1. Instalacion

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar `.env`

```bash
cp .env.example .env
```

Edita `.env` con tu token de bot y chat ID.

### 3. Ejecutar el Bot

```bash
python3 -m scripts.run_bot
```

El bot esta activo cuando ves:
```
INFO | services.telegram_bot_main | Bot iniciado. Escuchando comandos...
```

## Comandos Disponibles

### Comandos Base (pipeline DB)

| Comando | Descripcion | Ejemplo |
|---------|-------------|---------|
| `/start` | Muestra ayuda y comandos | `/start` |
| `/TASA` | Tasas actuales COP-VES, Zelle-Bs, USD-COP | `/TASA` |
| `/COP` | Mercado USDT-COP (compra, venta, spread) | `/COP` |
| `/VES` | Mercado USDT-VES (compra, venta, spread) | `/VES` |
| `/ARBITRAJE` | Analisis de oportunidades entre rutas | `/ARBITRAJE` |

### Comandos Analytics (pipeline RAM)

| Comando | Descripcion | Ejemplo |
|---------|-------------|---------|
| `/spread` | Analisis de spread del mercado | `/spread` |
| `/spread <posicion>` | Spread en posicion especifica | `/spread 5` |
| `/spread <desde> <hasta>` | Spread en rango de posiciones | `/spread 1 10` |
| `/merchant` | Top 10 merchants por volumen | `/merchant` |
| `/merchant buy` | Top compradores | `/merchant buy` |
| `/merchant sell` | Top vendedores | `/merchant sell` |
| `/merchant bots` | Detectar posibles bots | `/merchant bots` |
| `/merchant grandes` | Merchants con alto volumen | `/merchant grandes` |
| `/merchant search <texto>` | Buscar merchants por nombre | `/merchant search juan` |
| `/merchant @usuario` | Perfil detallado de un merchant | `/merchant @NombreUsuario` |
| `/volatilidad` | Analisis de volatilidad (5m/15m/1h) | `/volatilidad` |
| `/BUCKETS [par] [n]` | Ultimos n buckets agregados (10min) | `/BUCKETS USDT-COP 5` |

### Comandos Admin

| Comando | Descripcion | Ejemplo |
|---------|-------------|---------|
| `/auto_on [segundos]` | Envios automaticos cada N segundos (default 3600) | `/auto_on 1800` |
| `/auto_off` | Desactivar envios automaticos | `/auto_off` |

**Nota:** `/auto_on` y `/auto_off` solo funcionan desde el ID configurado en `OWNER_ID`.

## Arquitectura

```
Binance P2P API
    |
adapters/binance_p2p.py  (fetch con paginacion, hasta 100+ ads/lado)
    |
+--------------------------------------------------+
|  DOS PIPELINES PARALELOS:                         |
|                                                   |
|  PIPELINE A: DB-centric (legacy/scheduled)        |
|    core/fetcher.py -> core/db.py (raw_responses)  |
|    core/pipeline.py -> analisis desde DB           |
|    core/snapshot.py -> snapshots table             |
|    core/notifier.py -> formato + envio Telegram    |
|                                                   |
|  PIPELINE B: RAM in-memory (real-time)            |
|    core/ram_window.py -> ventana deslizante 6h     |
|    core/aggregator.py -> buckets 10min -> DB       |
|    core/detectors/ -> volatilidad, liquidez,       |
|                       actividad de merchants       |
|    services/analytics/ -> spread, merchant,        |
|                           volatilidad commands     |
+--------------------------------------------------+
    |
services/telegram_bot_main.py (11 handlers)
    |
Telegram (usuarios)
```

## Workers en Segundo Plano (Opcionales)

```bash
# En otra terminal: Worker de datos (fetch + RAM + aggregator)
python3 -m scripts.run_worker

# En otra terminal: Notificador automatico (envio periodico de tasas)
python3 -m scripts.run_notifier
```

## Base de Datos

Ubicacion: `data/p2p_data.db` (SQLite)

**Tablas:**
- `raw_responses` — respuestas crudas de la API Binance
- `snapshots` — resumenes agregados (precios, volatilidad, arbitraje)
- `aggregated_prices` — buckets de 10 minutos (avg, min, max, volumen, spread)
- `events` — senales y anomalias detectadas (volatilidad, liquidez, merchants)
- `merchant_stats` — estadisticas horarias por merchant

## Configuracion

La configuracion centralizada esta en `core/app_config.py`. Soporta override via variables de entorno:

- `WINDOW_SECONDS` — Ventana RAM en segundos (default: 21600 = 6h)
- `INGEST_MIN_ROWS` — Minimo de filas por ingest (default: 100)
- `DETECTOR_VOLATILITY_ENABLED` — Habilitar detector de volatilidad
- `DETECTOR_LIQUIDITY_ENABLED` — Habilitar detector de liquidez
- `DETECTOR_MERCHANT_ENABLED` — Habilitar detector de merchants
- `DETECTORS_CONFIG_JSON` — Override JSON completo para detectores

## Archivos Clave

- `services/telegram_bot_main.py` — Bot Telegram (11 handlers)
- `scripts/run_bot.py` — Entry point principal (worker + bot)
- `scripts/run_worker.py` — Worker de datos (scheduler + RAM + ingest)
- `core/app_config.py` — Configuracion centralizada
- `core/pipeline.py` — Analisis desde DB
- `core/notifier.py` — Formateo y envio de mensajes
- `core/ram_window.py` — Ventana deslizante in-memory
- `core/db.py` — Helpers SQLite
- `core/detectors/` — Detectores de senales (volatilidad, liquidez, merchants)
- `services/analytics/` — Comandos analytics (spread, merchant, volatilidad)

## Debugging

### El bot no responde?

```bash
# Verificar datos en DB
python3 -c "from core.db import fetch_latest_snapshots; print('Snapshots:', len(fetch_latest_snapshots(3)))"

# Obtener datos frescos
python3 -m scripts.fetch_and_store

# Generar snapshots
python3 -m scripts.generate_snapshot
```

### Error de BOT_TOKEN?

```bash
# Verificar .env
cat .env | grep BOT_TOKEN
# Debe ser: BOT_TOKEN=<tu_token> (sin espacios)
```

## Migraciones Realizadas

- CSV -> SQLite
- Arquitectura monolitica -> Modular (core/db, fetcher, pipeline, notifier)
- Bot integrado con DB
- Envios automaticos con scheduler
- Pipeline RAM in-memory con ventana deslizante
- Sistema de deteccion de senales (volatilidad, liquidez, merchants)
- Analytics avanzados (spread, merchant, volatilidad)

## Proximos Pasos

- [ ] API REST (FastAPI) para acceso HTTP
- [ ] Gestion de usuarios (Free/Premium)
- [ ] Soporte para otros exchanges
- [ ] Graficas historicas
- [ ] Alertas proactivas basadas en detectores
- [ ] Dashboard web

Ver [MIGRACION_RESUMEN.md](MIGRACION_RESUMEN.md) para mas detalles de arquitectura.
