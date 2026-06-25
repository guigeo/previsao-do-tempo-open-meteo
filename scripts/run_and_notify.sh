#!/usr/bin/env bash
# Roda a coleta diária via docker compose e notifica sucesso/falha via ntfy.sh.
# NTFY_TOPIC vem do .env (nunca commitado) — ver .env.example.
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="$PROJECT_DIR/logs/cron.log"

set -a
# shellcheck disable=SC1091
source "$PROJECT_DIR/.env"
set +a

mkdir -p "$PROJECT_DIR/logs"
cd "$PROJECT_DIR" || exit 1

{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
  /usr/bin/docker compose run --rm openmeteo
  echo "exit code: $?"
} >> "$LOG" 2>&1

EXIT_CODE=$(tail -5 "$LOG" | grep -oE 'exit code: [0-9]+' | tail -1 | grep -oE '[0-9]+$')

if [ -z "${NTFY_TOPIC:-}" ]; then
  echo "NTFY_TOPIC não configurado no .env — pulando notificação" >> "$LOG"
  exit "$EXIT_CODE"
fi

NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"

if [ "$EXIT_CODE" = "0" ]; then
  curl -s \
    -H "Title: previsao-tempo: coleta OK" \
    -H "Tags: white_check_mark" \
    -d "Coleta D-1 rodou com sucesso em $(date '+%Y-%m-%d %H:%M')" \
    "$NTFY_URL" > /dev/null
else
  TAIL=$(tail -15 "$LOG")
  curl -s \
    -H "Title: previsao-tempo: FALHA na coleta" \
    -H "Priority: urgent" \
    -H "Tags: rotating_light" \
    -d "Job falhou (exit $EXIT_CODE) em $(date '+%Y-%m-%d %H:%M'). Últimas linhas:
$TAIL" \
    "$NTFY_URL" > /dev/null
fi
