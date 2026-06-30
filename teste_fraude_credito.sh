#!/bin/bash
# =============================================================================
# teste_fraude_credito.sh
# Demo: zona tenta burlar saldo — sistema rejeita bloco e nega alocação de drone
#
# Uso:
#   bash teste_fraude_credito.sh [zona] [peer_http] [peer_tcp]
#
# Exemplos (portas mapeadas pelo docker-compose):
#   bash teste_fraude_credito.sh NORTE  localhost:8081 localhost:9090
#   bash teste_fraude_credito.sh SUL    localhost:8082 localhost:9091
#   bash teste_fraude_credito.sh LESTE  localhost:8083 localhost:9092
# =============================================================================

ZONA="${1:-NORTE}"
PEER_HTTP="${2:-localhost:8081}"
PEER_TCP="${3:-localhost:9090}"
PEER_TCP_HOST="${PEER_TCP%%:*}"
PEER_TCP_PORT="${PEER_TCP##*:}"

VERDE="\033[0;32m"; VERMELHO="\033[0;31m"; AMARELO="\033[1;33m"
CIANO="\033[0;36m"; NEGRITO="\033[1m"; RESET="\033[0m"

sep()  { echo -e "${CIANO}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"; }
ok()   { echo -e "  ${VERDE}✅ $1${RESET}"; }
fail() { echo -e "  ${VERMELHO}❌ $1${RESET}"; }
info() { echo -e "  ${AMARELO}ℹ  $1${RESET}"; }

echo ""
echo -e "${NEGRITO}╔══════════════════════════════════════════════════╗${RESET}"
echo -e "${NEGRITO}║     TESTE DE FRAUDE DE CRÉDITO — PBL-2 P3       ║${RESET}"
echo -e "${NEGRITO}╚══════════════════════════════════════════════════╝${RESET}"
echo -e "  Zona alvo : ${NEGRITO}${ZONA}${RESET}"
echo -e "  HTTP      : ${PEER_HTTP}"
echo -e "  TCP       : ${PEER_TCP}"
echo ""

# ---------------------------------------------------------------------------
# Consulta saldo via /status
# ---------------------------------------------------------------------------
saldo_zona() {
    curl -sf "http://${1}/status" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('saldos',{}).get('${ZONA}','?'))" 2>/dev/null \
        || echo "?"
}

# ---------------------------------------------------------------------------
# Envia bloco forjado identificando-se como SENSOR (não afeta o Ricart)
# ---------------------------------------------------------------------------
injetar_bloco_falso() {
    local creditos="$1"
    local txid="fraude-$(date +%s)-$$"
    local agora
    agora=$(python3 -c "from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat())")

    local msg
    msg=$(python3 -c "
import json

tx = {
    'tipo': 'PAGAMENTO',
    'zona_id': '${ZONA}',
    'creditos': ${creditos},
    'drone_id': 'drone-fantasma',
    'ocorrencia': 'fraude_teste',
    'zona': '${ZONA}',
    'timestamp': '${agora}',
    'tx_id': '${txid}',
    'assinatura': 'assinatura_falsa_' + 'a'*64,
    'chave_publica': 'chave_falsa_' + 'b'*64
}
bloco = {
    'index': 9999,
    'timestamp': '${agora}',
    'transacao': tx,
    'hash_anterior': '0'*64,
    'hash': '00' + 'c'*62,
    'nonce': 0,
    'minerador': '${ZONA}'
}
print(json.dumps({'tipo': 'BLOCO', 'de': '${ZONA}', 'para': '', 'dados': bloco, 'timestamp': '${agora}'}))
")

    # Identifica como SENSOR — não entra na tabela de peers, não afeta o Ricart
    (
        printf "IAM:SENSOR:sensor-fraude-teste\n"
        sleep 0.3
        printf "%s\n" "$msg"
        sleep 1
    ) | nc -q 2 "$PEER_TCP_HOST" "$PEER_TCP_PORT" 2>/dev/null
}

# ---------------------------------------------------------------------------
# PASSO 1 — Saldo real antes
# ---------------------------------------------------------------------------
sep
echo -e "${NEGRITO}PASSO 1 — Saldo legítimo atual${RESET}"
sep

SALDO_ANTES=$(saldo_zona "$PEER_HTTP")
if [ "$SALDO_ANTES" = "?" ]; then
    fail "Não foi possível consultar /status em ${PEER_HTTP}. O sistema está rodando?"
    exit 1
fi
ok "Saldo de ${ZONA} = ${NEGRITO}${SALDO_ANTES} créditos${RESET}"

# ---------------------------------------------------------------------------
# PASSO 2 — Injeta bloco com créditos falsos e assinatura inválida
# ---------------------------------------------------------------------------
sep
echo -e "${NEGRITO}PASSO 2 — Tentativa de fraude: bloco com +99999 créditos e assinatura falsa${RESET}"
sep

info "Enviando bloco PAGAMENTO forjado (sem assinatura Ed25519 válida)..."
injetar_bloco_falso 99999
sleep 2

SALDO_APOS=$(saldo_zona "$PEER_HTTP")
ok "Saldo de ${ZONA} após tentativa = ${NEGRITO}${SALDO_APOS} créditos${RESET}"

if [ "$SALDO_APOS" = "$SALDO_ANTES" ]; then
    ok "Bloco falso REJEITADO — saldo permaneceu ${SALDO_ANTES} ✓"
else
    fail "FALHA DE SEGURANÇA: saldo mudou de ${SALDO_ANTES} para ${SALDO_APOS}!"
fi

# ---------------------------------------------------------------------------
# PASSO 3 — Requisição legítima (mostra que o sistema continua operando)
# ---------------------------------------------------------------------------
sep
echo -e "${NEGRITO}PASSO 3 — Requisição legítima de drone (sistema deve aceitar)${RESET}"
sep

AGORA=$(python3 -c "from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat())")
REQ=$(python3 -c "
import json
print(json.dumps({
    'tipo': 'REQUISICAO_DRONE',
    'sensor': 'sensor-teste-fraude',
    'zona_id': '${ZONA}',
    'zona': '${ZONA}',
    'ocorrencia': 'inspecao_visual_urgente',
    'prioridade': 3,
    'timestamp': '${AGORA}',
    'pago': False
}))
")

info "Enviando requisição legítima via TCP..."
(
    printf "IAM:SENSOR:sensor-teste-fraude\n"
    sleep 0.3
    printf "%s\n" "$REQ"
    sleep 2
) | nc -q 3 "$PEER_TCP_HOST" "$PEER_TCP_PORT" 2>/dev/null

sleep 3
SALDO_FINAL=$(saldo_zona "$PEER_HTTP")
ok "Saldo de ${ZONA} após requisição = ${NEGRITO}${SALDO_FINAL} créditos${RESET}"

if [ "$SALDO_FINAL" != "$SALDO_APOS" ] && [[ "$SALDO_FINAL" =~ ^[0-9]+$ ]]; then
    ok "Débito de 10 créditos registrado no ledger — alocação funcionando ✓"
else
    info "Saldo não mudou (drone pode estar ocupado ou fila cheia — normal)"
fi

# ---------------------------------------------------------------------------
# PASSO 4 — Integridade da chain
# ---------------------------------------------------------------------------
sep
echo -e "${NEGRITO}PASSO 4 — Integridade da chain após tentativa de fraude${RESET}"
sep

VALIDATE=$(curl -sf "http://${PEER_HTTP}/validate" 2>/dev/null)
VALIDO=$(echo "$VALIDATE" | python3 -c "import sys,json; d=json.load(sys.stdin); print('SIM' if d.get('valida') else 'NAO')" 2>/dev/null || echo "?")
TOTAL=$(echo "$VALIDATE"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total_blocos','?'))" 2>/dev/null || echo "?")

if [ "$VALIDO" = "SIM" ]; then
    ok "Chain íntegra — ${TOTAL} blocos, todos os hashes conferem ✓"
else
    fail "Chain inválida! Motivo: $(echo "$VALIDATE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('motivo','?'))" 2>/dev/null)"
fi

# ---------------------------------------------------------------------------
# RESUMO
# ---------------------------------------------------------------------------
sep
echo -e "${NEGRITO}╔══════════════════════════════════════════════════╗${RESET}"
echo -e "${NEGRITO}║                    RESUMO                       ║${RESET}"
echo -e "${NEGRITO}╚══════════════════════════════════════════════════╝${RESET}"
echo -e "  Saldo antes da fraude : ${NEGRITO}${SALDO_ANTES}${RESET} créditos"
echo -e "  Saldo após fraude     : ${NEGRITO}${SALDO_APOS}${RESET} créditos  (deve ser igual ao anterior)"
echo -e "  Saldo após req. legít.: ${NEGRITO}${SALDO_FINAL}${RESET} créditos  (deve ser 10 a menos, se drone livre)"
echo -e "  Chain válida          : ${NEGRITO}${VALIDO}${RESET}"
echo ""
if [ "$SALDO_APOS" = "$SALDO_ANTES" ] && [ "$VALIDO" = "SIM" ]; then
    echo -e "  ${VERDE}${NEGRITO}SISTEMA SEGURO — fraude bloqueada, ledger íntegro ✓${RESET}"
else
    echo -e "  ${VERMELHO}${NEGRITO}ATENÇÃO — revisar implementação${RESET}"
fi
echo ""
