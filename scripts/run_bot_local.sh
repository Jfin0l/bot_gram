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

echo "Starting bot (PYTHONPATH=. python3 -m scripts.run_bot)"
PYTHONPATH=. python3 -m scripts.run_bot
