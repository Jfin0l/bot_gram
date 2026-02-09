# Resumen de Reestructuración — FastMoney Bot P2P

## Logros Completados

### Arquitectura Migrada a DB-Centric
- **Antes**: Archivos CSV, requests directos en la API, acoplamiento entre fetch, análisis y formateo.
- **Ahora**: SQLite centralizado (`data/p2p_data.db`) con separación clara de responsabilidades:
  1. Fetcher (obtiene datos)
  2. Storage (persiste raw responses)
  3. Pipeline (analiza y construye resultados)
  4. Notifier (formatea y envía mensajes)

### Módulos Nuevos Implementados
| Módulo | Propósito |
|--------|-----------|
| `core/db.py` | Helpers SQLite: `save_raw_response`, `save_snapshot_summary`, `fetch_latest_raw`, query helpers |
| `core/fetcher.py` | Orquestador: 1 request por `fiat`/`tradeType` → guarda raw en DB |
| `core/pipeline.py` | Construye análisis a partir de raw almacenado en DB |
| `core/snapshot.py` | Genera snapshots resumidos desde pipeline |
| `core/scheduler.py` | BackgroundScheduler para ejecutar jobs periódicamente |
| `core/notifier.py` | Formatters, send_message async, scheduler de envíos a Telegram |

### Runners (Scripts) Disponibles
```bash
# Obtener y persistir datos raw (una ejecución)
python3 -m scripts.fetch_and_store

# Generar snapshots desde raw en DB
python3 -m scripts.generate_snapshot

# Iniciar worker: fetch + snapshot scheduler (600s intervalo)
python3 -m scripts.run_worker

# Notifier: scheduler de envíos automáticos a Telegram
python3 -m scripts.run_notifier

# Test: envío puntual a Telegram
python3 -m scripts.test_send_telegram
```

### Base de Datos
**Tablas creadas en `data/p2p_data.db`:**
- `raw_responses`: respuestas crudas de la API (exchange, fiat, tradeType, raw_json)
- `snapshots`: resúmenes agregados por par (pair, precios, coef_var, etc.)

**Índices**: `pair`, `timestamp_utc` para queries rápidas.

### Integración Telegram
| Feature | Estado |
|---------|--------|
| Envío de mensajes HTTP | ✅ Funciona (async/await) |
| Formatter `/TASA` | ✅ Implementado |
| Formatter `/COP` y `/VES` | ✅ Implementado |
| Scheduler automático | ✅ Implementado |
| Dry-run (sin token) | ✅ Soportado |

### Pruebas Ejecutadas
✅ Integración completa: fetch → persistir → snapshot → formateo → envío a Telegram  
✅ Scheduler jobs ejecutados sin errores  
✅ Envío real a canal Telegram verificado  

## Configuración Requerida

### `.env` (raíz del proyecto)
```
BOT_TOKEN=<tu_token_aqui>
CHAT_ID=<id_del_canal_o_grupo>
OWNER_ID=<tu_id_telegram>
```

## Lo Que Falta para Nivel Básico Completo

1. **Comandos Telegram Interactivos** (`/TASA`, `/COP`, `/VES`, etc.)
   - Actualmente: `core/notifier.py` solo tiene formatters
   - Próximo paso: Crear `services/telegram_bot.py` con handlers de comandos que lean desde DB

2. **API REST** (para futuro acceso a datos por web)
   - Sugerido: FastAPI + Pydantic para endpoints de snapshots y análisis

3. **Gestión de Usuarios** (Free vs Premium)
   - Requerirá tabla adicional en DB: `users` (user_id, plan, created_at, etc.)

4. **Recolección de Datos Históricos**
   - Script para migrar datos viejos (si existen CSVs)
   - Actualmente: Los CSVs fueron eliminados en la migración

5. **Monitoring / Alertas**
   - Fallos en fetch/notifier
   - Volatilidad extrema en mercados
   - Uptime checks

## Próximos Pasos Sugeridos

### Corto Plazo (Semana 1)
- [ ] Crear `services/telegram_bot.py` con handlers de comandos
- [ ] Integrar handlers con `core/notifier.py`
- [ ] Crear systemd services para `run_worker` y `run_notifier`

### Mediano Plazo (Semana 2-3)
- [ ] Añadir tabla `markets` para soportar múltiples exchanges (no solo Binance)
- [ ] Crear adaptadores para otros P2P (OKEx, Gate.io, etc.)
- [ ] API REST con FastAPI

### Largo Plazo (Mes 2+)
- [ ] Gestión de usuarios y planes (Free/Premium)
- [ ] Web UI para consultas de datos
- [ ] Machine learning para predicción de tendencias
- [ ] Comercialización de datos de inteligencia de mercado

## Estructura Actual de Carpetas
```
bot_gram-1/
├── core/              # Lógica compartida
│   ├── db.py         # SQLite helpers
│   ├── fetcher.py    # Obtener datos
│   ├── pipeline.py   # Analizar datos
│   ├── snapshot.py   # Resumir snapshots
│   ├── scheduler.py  # Jobs periódicos
│   ├── notifier.py   # Formatear + enviar mensajes
│   └── storage.py    # Persistencia (legacy, ahora usa DB)
├── adapters/         # Conectores de mercados
│   └── binance_p2p.py
├── services/         # Servicios (Telegram, API, etc.)
│   └── telegram_bot.py (próximo)
├── scripts/          # Runners
│   ├── fetch_and_store.py
│   ├── generate_snapshot.py
│   ├── run_worker.py
│   ├── run_notifier.py
│   ├── test_send_telegram.py
│   └── test_scheduler.py
├── data/
│   ├── p2p_data.db   # Base de datos central
│   └── snapshots/    # (antiguo, ahora en DB)
├── config.py         # Configuración global
├── p2p_info.py       # Código original (referencia, se puede deprecar)
└── requirements.txt  # Dependencias
```

## Notas Importantes

- **p2p_info.py**: Contiene código original de referencia. Ahora está desacoplado del nuevo sistema. Se puede deprecar/remover cuando los handlers Telegram migren a `services/telegram_bot.py`.
- **CSV Migration**: Todos los CSVs fueron eliminados. Los datos ahora residen en SQLite.
- **Async/Await**: `core/notifier.py` usa asyncio con `python-telegram-bot 21.10+` (async-first).
- **Escalabilidad**: La estructura soporta múltiples exchanges, mercados y usuarios sin cambios mayores.

## Testing Rápido
```bash
# Importar y probar pipeline directamente
python3 - <<'PY'
from core import pipeline, notifier, db
cfg = {"monedas":{"COP":{"rows":20,"page":2},"VES":{"rows":20,"page":4}},"filas_tasa_remesa":5,"ponderacion_volumen":True,"limite_outlier":0.025}
data = pipeline.build_data_from_db(cfg)
print(notifier.format_tasa(data))
PY
```

---
**Fecha actualizada**: 2026-02-08  
**Estado**: Prototipo Base Funcional ✅
