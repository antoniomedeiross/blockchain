#!/bin/sh
# ─────────────────────────────────────────────────────────────────────────────
# entrypoint.sh — gera index.html a partir de DRONE_PEERS e sobe o servidor
#
# Variável esperada:
#   DRONE_PEERS="NOME1=IP1:PORTA1,NOME2=IP2:PORTA2,NOME3=IP3:PORTA3,..."
#
# Exemplos válidos:
#   DRONE_PEERS="NORTE=192.168.1.10:8080,SUL=192.168.1.11:8080,LESTE=192.168.1.12:8080"
#   DRONE_PEERS="A=10.0.0.1:5000,B=10.0.0.2:5000,C=10.0.0.3:5000,D=10.0.0.4:5000"
#
# Rota de status de cada peer:
#   DRONE_STATUS_PATH — opcional, default "/status"
# ─────────────────────────────────────────────────────────────────────────────

set -e

TEMPLATE="/app/index.html.template"
OUTPUT="/app/index.html"
STATUS_PATH="${DRONE_STATUS_PATH:-/status}"

# ── Validação ────────────────────────────────────────────────────────────────
if [ -z "$DRONE_PEERS" ]; then
  echo "[entrypoint] ERRO: variável DRONE_PEERS não definida."
  echo "  Exemplo: DRONE_PEERS=\"NORTE=192.168.1.10:8080,SUL=192.168.1.11:8080\""
  exit 1
fi

# ── Converte DRONE_PEERS em array JS ─────────────────────────────────────────
# Entrada : "NORTE=192.168.1.10:8080,SUL=192.168.1.11:8080"
# Saída   : [{"zona":"NORTE","url":"http://192.168.1.10:8080/status"},...]

peers_js="["
first=1

# Divide por vírgula
IFS=',' 
for entry in $DRONE_PEERS; do
  # Divide por '=' (apenas a primeira ocorrência)
  zona="${entry%%=*}"
  addr="${entry#*=}"

  # Adiciona http:// se não tiver esquema
  case "$addr" in
    http://*|https://*) url="${addr}${STATUS_PATH}" ;;
    *)                  url="http://${addr}${STATUS_PATH}" ;;
  esac

  if [ $first -eq 0 ]; then
    peers_js="${peers_js},"
  fi
  peers_js="${peers_js}{\"zona\":\"${zona}\",\"url\":\"${url}\"}"
  first=0
done
unset IFS

peers_js="${peers_js}]"

echo "[entrypoint] Peers carregados:"
echo "$DRONE_PEERS" | tr ',' '\n' | while IFS='=' read -r z a; do
  echo "  → $z  ($a)"
done
echo ""

# ── Injeta no template via sed ────────────────────────────────────────────────
# Usa delimitador | para não conflitar com / nos URLs
sed "s|\\${PEERS_JS}|${peers_js}|g" "$TEMPLATE" > "$OUTPUT"

echo "[entrypoint] index.html gerado com ${#DRONE_PEERS} bytes de config."
echo "[entrypoint] Subindo servidor na porta ${PORT:-8080}..."

# ── Inicia o servidor HTTP ────────────────────────────────────────────────────
# Troca pelo seu servidor real se necessário (nginx, node, python, etc.)
exec "$@"