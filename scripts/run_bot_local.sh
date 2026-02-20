#!/usr/bin/env bash
# Helper to run the bot locally loading .env variables.
set -euo pipefail

if [ ! -f .env ]; then
  echo ".env file not found. Copy .env.example and fill BOT_TOKEN, CHAT_ID, OWNER_ID"
  exit 1
fi

# export all env vars from .env
set -a
source .env
set +a

# Prefer project virtualenv if present
VENV_PY=".venv/bin/python"
if [ -x "$VENV_PY" ]; then
  PYTHON_EXEC="$VENV_PY"
else
  PYTHON_EXEC="python3"
fi

echo "Starting bot with $PYTHON_EXEC (PYTHONPATH=. )"
PYTHONPATH=. "$PYTHON_EXEC" -m scripts.run_bot
