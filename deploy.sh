#!/bin/bash

# Configuration
APP_NAME="bot_gram"
VENV_DIR=".venv"
MARKER_FILE=".requirements_installed"
DB_FILE="data/p2p_data.db"
ENV_FILE=".env"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

function print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1. Limpieza inicial
print_status "Limpiando caches de Python..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# 2. Gestión de Entorno Virtual
if [ ! -d "$VENV_DIR" ]; then
    print_warn "Entorno virtual no encontrado. Creando..."
    python3 -m venv $VENV_DIR
fi

source $VENV_DIR/bin/activate

# 3. Instalación de dependencias si es necesario
if [ ! -f "$MARKER_FILE" ] || [ "requirements.txt" -nt "$MARKER_FILE" ]; then
    print_status "requirements.txt ha cambiado o es una nueva instalación. Instalando dependencias..."
    pip install --upgrade pip
    pip install -r requirements.txt
    touch "$MARKER_FILE"
else
    print_status "Las dependencias están al día."
fi

# 4. Verificación de Archivos Críticos
if [ ! -f "$ENV_FILE" ]; then
    print_error "Archivo .env no encontrado. El bot no podrá iniciar sin credenciales."
    # No detenemos el script para permitir otros comandos como 'stop' o 'logs', 
    # pero el 'start' fallará por lógica del bot.
fi

# 5. Migraciones e Integridad de DB
print_status "Ejecutando script de integridad de Base de Datos..."
export PYTHONPATH=$PYTHONPATH:$(pwd)
python3 scripts/maintain_db.py
if [ $? -ne 0 ]; then
    print_error "Fallo en la verificación de la Base de Datos. Abortando despliegue."
    exit 1
fi

API_NAME="api_gram"

# 6. Lógica de PM2
function start_app() {
    # Proceso 1: Bot de Telegram
    pm2 describe $APP_NAME > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_status "Recargando Bot ($APP_NAME)..."
        pm2 reload $APP_NAME
    else
        print_status "Iniciando Bot ($APP_NAME)..."
        pm2 start scripts/run_bot.py --name $APP_NAME --interpreter ./.venv/bin/python3
    fi

    # Proceso 2: API FastAPI (Uvicorn)
    pm2 describe $API_NAME > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_status "Recargando API ($API_NAME)..."
        pm2 reload $API_NAME
    else
        print_status "Iniciando API ($API_NAME)..."
        # Usamos el python del venv directamente para ejecutar uvicorn como módulo
        pm2 start "./.venv/bin/python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000" --name $API_NAME
    fi
    
    pm2 save
}

function stop_app() {
    print_status "Deteniendo servicios..."
    pm2 stop $APP_NAME
    pm2 stop $API_NAME
}

function show_logs() {
    pm2 logs --lines 50
}

function show_status() {
    pm2 list
    pm2 monit
}

# 7. Manejo de argumentos
case "$1" in
    stop)
        stop_app
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    restart)
        stop_app
        start_app
        ;;
    *)
        # Default action is deploy/start
        start_app
        ;;
esac

exit 0
