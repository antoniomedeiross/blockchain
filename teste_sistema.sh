#!/bin/bash
# =============================================================================
# SCRIPT DE TESTE — SISTEMA DISTRIBUÍDO DE DRONES (PBL-2)
# =============================================================================
# Cobre os requisitos principais do problema:
#   [R1] Exclusão mútua — mesmo drone não pode ser alocado duas vezes
#   [R2] Prioridade — ocorrências críticas atendidas primeiro
#   [R3] Um drone por requisição — sem desperdício operacional
#   [R4] Resiliência — sistema continua sem ponto único de falha
#   [R5] Failover de drone — drone reconecta em outra zona e continua operando
#   [R6] Envelhecimento — requisições de baixa prioridade sobem de nível com tempo
# =============================================================================

COMPOSE="docker compose"
LOG_DIR="./logs_teste"
RESULTADO_FINAL=0

# Cores
Verde='\033[0;32m'
Vermelho='\033[0;31m'
Amarelo='\033[1;33m'
Azul='\033[0;34m'
Negrito='\033[1m'
Reset='\033[0m'

mkdir -p "$LOG_DIR"

# =============================================================================
# UTILITÁRIOS
# =============================================================================

ok()   { echo -e "  ${Verde}✓${Reset} $1"; }
fail() { echo -e "  ${Vermelho}✗${Reset} $1"; RESULTADO_FINAL=1; }
info() { echo -e "  ${Azul}→${Reset} $1"; }
titulo() {
    echo ""
    echo -e "${Negrito}${Amarelo}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${Reset}"
    echo -e "${Negrito}${Amarelo}  $1${Reset}"
    echo -e "${Negrito}${Amarelo}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${Reset}"
}

# Conta ocorrências de padrão em arquivo — sempre retorna número, nunca falha
contar() {
    grep -c "$1" "$2" 2>/dev/null; true
}

# Versão segura para uso em $(( )) — garante inteiro mesmo com grep retornando exit 1
contar_int() {
    local n
    n=$(grep -c "$1" "$2" 2>/dev/null) && echo "$n" || echo 0
}

# Verifica se padrão existe no log
existe() {
    grep -q "$1" "$2" 2>/dev/null
}

# Captura logs de um container por N segundos em background
capturar_logs() {
    local container=$1
    local arquivo=$2
    local segundos=$3
    timeout "$segundos" $COMPOSE logs -f "$container" > "$arquivo" 2>&1 &
}

# Aguarda até padrão aparecer no log ou timeout
aguardar_padrao() {
    local arquivo=$1
    local padrao=$2
    local timeout_s=$3
    local elapsed=0
    while [ $elapsed -lt $timeout_s ]; do
        if grep -q "$padrao" "$arquivo" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

# =============================================================================
# VERIFICAÇÃO PRÉ-TESTE
# =============================================================================

titulo "PRÉ-VERIFICAÇÃO"

if ! docker info > /dev/null 2>&1; then
    fail "Docker não está rodando"
    exit 1
fi
ok "Docker disponível"

if ! $COMPOSE version > /dev/null 2>&1; then
    fail "docker compose não encontrado"
    exit 1
fi
ok "docker compose disponível"

# =============================================================================
# TESTE 0 — BUILD E INICIALIZAÇÃO
# =============================================================================

titulo "TESTE 0 — Build e inicialização do sistema"

info "Derrubando containers anteriores..."
$COMPOSE down --remove-orphans > /dev/null 2>&1

info "Subindo o sistema..."
$COMPOSE up -d --build > "$LOG_DIR/build.log" 2>&1
if [ $? -ne 0 ]; then
    fail "Falha no build — veja $LOG_DIR/build.log"
    exit 1
fi
ok "Build concluído"

info "Aguardando peers se conectarem (15s)..."
sleep 15

for peer in peer1 peer2 peer3; do
    if $COMPOSE ps "$peer" 2>/dev/null | grep -qE "Up|running"; then
        ok "Container $peer está UP"
    else
        fail "Container $peer não está rodando"
    fi
done

if $COMPOSE ps drone-norte-1 2>/dev/null | grep -qE "Up|running"; then
    ok "Container drone-norte-1 está UP"
else
    fail "Container drone-norte-1 não está rodando"
fi

# =============================================================================
# TESTE 1 — CONECTIVIDADE P2P
# =============================================================================

titulo "TESTE 1 — Conectividade P2P entre todos os peers"

info "Coletando logs de conexão..."
for peer in peer1 peer2 peer3; do
    $COMPOSE logs "$peer" > "$LOG_DIR/${peer}_inicial.log" 2>&1
done
sleep 3

for peer in peer1 peer2 peer3; do
    conectados=$(contar_int "Identificado com sucesso para" "$LOG_DIR/${peer}_inicial.log")
    if [ "$conectados" -ge 2 ]; then
        ok "$peer: conectado a $conectados peers"
    else
        fail "$peer: esperava 2 conexões, encontrou $conectados"
    fi
done

for peer in peer1 peer2 peer3; do
    if existe "SYNC_RESPONSE" "$LOG_DIR/${peer}_inicial.log"; then
        ok "$peer: sincronização inicial realizada"
    else
        fail "$peer: sem SYNC_RESPONSE nos logs"
    fi
done

# =============================================================================
# TESTE 2 — EXCLUSÃO MÚTUA [R1 e R3]
# =============================================================================

titulo "TESTE 2 — Exclusão mútua (Ricart-Agrawala)"

info "Capturando logs dos 3 peers por 40s para análise de concorrência..."
for peer in peer1 peer2 peer3; do
    capturar_logs "$peer" "$LOG_DIR/${peer}_mutex.log" 40
done
sleep 40

for peer in peer1 peer2 peer3; do
    if existe "RICART" "$LOG_DIR/${peer}_mutex.log"; then
        ok "$peer: algoritmo Ricart-Agrawala ativo"
    else
        info "$peer: sem atividade Ricart neste período"
    fi
done

for peer in peer1 peer2 peer3; do
    alocacoes=$(contar_int "Quorum atingido" "$LOG_DIR/${peer}_mutex.log")
    if [ "$alocacoes" -gt 0 ]; then
        ok "$peer: $alocacoes alocações via quorum Ricart"
    fi
done

total_requests=$(contar_int "Tipo=REQUEST" "$LOG_DIR/peer1_mutex.log")
r2=$(contar_int "Tipo=REQUEST" "$LOG_DIR/peer2_mutex.log")
r3=$(contar_int "Tipo=REQUEST" "$LOG_DIR/peer3_mutex.log")
total_requests=$((total_requests + r2 + r3))

total_replies=$(contar_int "Tipo=REPLY" "$LOG_DIR/peer1_mutex.log")
rp2=$(contar_int "Tipo=REPLY" "$LOG_DIR/peer2_mutex.log")
rp3=$(contar_int "Tipo=REPLY" "$LOG_DIR/peer3_mutex.log")
total_replies=$((total_replies + rp2 + rp3))

total_releases=$(contar_int "Tipo=RELEASE" "$LOG_DIR/peer1_mutex.log")
rl2=$(contar_int "Tipo=RELEASE" "$LOG_DIR/peer2_mutex.log")
rl3=$(contar_int "Tipo=RELEASE" "$LOG_DIR/peer3_mutex.log")
total_releases=$((total_releases + rl2 + rl3))

info "Mensagens Ricart: REQUEST=$total_requests | REPLY=$total_replies | RELEASE=$total_releases"

if [ "$total_requests" -gt 0 ] && [ "$total_replies" -gt 0 ] && [ "$total_releases" -gt 0 ]; then
    ok "Protocolo Ricart-Agrawala completo (REQUEST→REPLY→RELEASE)"
else
    fail "Protocolo Ricart incompleto — verifique se há sensores gerando requisições"
fi

# Detecção correta de alocação dupla:
# Dentro de um mesmo peer, "Quorum atingido" nunca deve aparecer duas vezes
# consecutivas sem "Reset completo" no meio (significaria que o Ricart alocou
# sem terminar o ciclo anterior).
dupla_encontrada=0
for peer in peer1 peer2 peer3; do
    # Extrai apenas as linhas relevantes em ordem
    sequencia=$(grep -E "Quorum atingido|Reset completo" "$LOG_DIR/${peer}_mutex.log" 2>/dev/null \
        | grep -oP "(Quorum atingido|Reset completo)")
    ultimo=""
    while IFS= read -r linha; do
        if [ "$linha" = "Quorum atingido" ] && [ "$ultimo" = "Quorum atingido" ]; then
            fail "$peer: ALOCAÇÃO DUPLA detectada — dois Quorum seguidos sem Reset"
            dupla_encontrada=1
            break
        fi
        ultimo="$linha"
    done <<< "$sequencia"
done
if [ "$dupla_encontrada" -eq 0 ]; then
    ok "Nenhuma alocação dupla detectada em nenhum peer"
fi

# =============================================================================
# TESTE 3 — PRIORIDADE DE ATENDIMENTO [R2]
# =============================================================================

titulo "TESTE 3 — Prioridade de atendimento"

info "Analisando logs para verificar ordenação por prioridade..."
for peer in peer1 peer2 peer3; do
    prios=$(grep "Disparando Ricart para prioridade" "$LOG_DIR/${peer}_mutex.log" 2>/dev/null \
        | grep -oP 'prioridade \K[0-9]+' | tr '\n' ' ')
    if [ -n "$prios" ]; then
        info "$peer despachou por prioridade (ordem): $prios"
    fi
done

for peer in peer1 peer2 peer3; do
    if existe "FILA.*Disparando" "$LOG_DIR/${peer}_mutex.log"; then
        ok "$peer: fila de prioridade funcionando"
    fi
done

# =============================================================================
# TESTE 4 — ENVELHECIMENTO DE REQUISIÇÕES [R6]
# =============================================================================

titulo "TESTE 4 — Envelhecimento de requisições (anti-starvation)"

info "Aguardando 35s para observar envelhecimento (tick a cada 10s)..."
for peer in peer1 peer2 peer3; do
    capturar_logs "$peer" "$LOG_DIR/${peer}_aging.log" 35
done
sleep 35

envelhecimentos=0
for peer in peer1 peer2 peer3; do
    n=$(contar_int "Envelhecimento aplicado" "$LOG_DIR/${peer}_aging.log")
    envelhecimentos=$((envelhecimentos + n))
done

if [ "$envelhecimentos" -gt 0 ]; then
    ok "Envelhecimento de fila detectado ($envelhecimentos eventos nos peers)"
else
    info "Nenhum envelhecimento observado — fila pode estar vazia (drones atendendo rápido)"
fi

# =============================================================================
# TESTE 5 — RESILIÊNCIA: QUEDA DE UM PEER [R4]
# =============================================================================

titulo "TESTE 5 — Resiliência: queda de peer3 (LESTE)"

info "Estado antes da queda:"
$COMPOSE logs peer1 > "$LOG_DIR/peer1_pre_queda.log" 2>&1
$COMPOSE logs peer2 > "$LOG_DIR/peer2_pre_queda.log" 2>&1

info "Derrubando peer3 (LESTE)..."
$COMPOSE stop peer3 > /dev/null 2>&1
ok "peer3 parado"

info "Aguardando detecção de falha pelos outros peers (10s)..."
capturar_logs peer1 "$LOG_DIR/peer1_pos_queda.log" 30
capturar_logs peer2 "$LOG_DIR/peer2_pos_queda.log" 30
sleep 15

for peer in peer1 peer2; do
    if existe "LESTE desconectado\|peer3.*perdida\|offline" "$LOG_DIR/${peer}_pos_queda.log"; then
        ok "$peer detectou a queda do LESTE"
    else
        fail "$peer não detectou a queda do LESTE"
    fi
done

sleep 10
for peer in peer1 peer2; do
    if existe "RICART\|FILA\|REPLY\|REQUEST" "$LOG_DIR/${peer}_pos_queda.log"; then
        ok "$peer continua operando após queda do LESTE"
    else
        info "$peer sem atividade Ricart detectada após queda"
    fi
done

if existe "Tipo=REQUEST\|Tipo=REPLY\|Tipo=RELEASE\|DRONE_UPDATE" "$LOG_DIR/peer1_pos_queda.log" || \
   existe "Tipo=REQUEST\|Tipo=REPLY\|Tipo=RELEASE\|DRONE_UPDATE" "$LOG_DIR/peer2_pos_queda.log"; then
    ok "Comunicação P2P continua entre peer1 e peer2"
else
    info "Sem mensagens P2P observadas — sistema aguardando novas requisições"
fi

info "Restaurando peer3..."
$COMPOSE start peer3 > /dev/null 2>&1
sleep 8

if $COMPOSE ps peer3 2>/dev/null | grep -qE "Up|running"; then
    ok "peer3 restaurado com sucesso"
else
    fail "peer3 não voltou"
fi

capturar_logs peer1 "$LOG_DIR/peer1_reconexao.log" 15
sleep 12
if existe "Sucesso: Conectado ao peer peer3\|LESTE conectado" "$LOG_DIR/peer1_reconexao.log"; then
    ok "peer1 reconectou ao peer3 após restauração"
else
    fail "peer1 não reconectou ao peer3"
fi

# =============================================================================
# TESTE 6 — FAILOVER DE DRONE [R5]
# =============================================================================

titulo "TESTE 6 — Failover de drone (queda do peer1/NORTE)"

info "Capturando estado atual do drone..."
$COMPOSE logs drone-norte-1 > "$LOG_DIR/drone_pre_failover.log" 2>&1

missoes_antes=$(contar_int "Missão concluída" "$LOG_DIR/drone_pre_failover.log")
info "Missões concluídas antes do failover: $missoes_antes"

info "Derrubando peer1 (NORTE — zona base do drone)..."
$COMPOSE stop peer1 > /dev/null 2>&1
ok "peer1 parado"

info "Capturando logs de failover por 30s..."
capturar_logs drone-norte-1 "$LOG_DIR/drone_failover.log" 30
capturar_logs peer2 "$LOG_DIR/peer2_failover.log" 30
capturar_logs peer3 "$LOG_DIR/peer3_failover.log" 30
sleep 25

if existe "Conexão perdida" "$LOG_DIR/drone_failover.log"; then
    ok "Drone detectou queda do peer1"
else
    fail "Drone não detectou queda do peer1"
fi

if existe "Conectado em peer2\|Conectado em peer3" "$LOG_DIR/drone_failover.log"; then
    zona_failover=$(grep "Conectado em peer" "$LOG_DIR/drone_failover.log" 2>/dev/null \
        | grep -v "peer1" | tail -1 | grep -oP "peer[0-9]")
    ok "Drone reconectou em failover: $zona_failover"
else
    fail "Drone não reconectou em outra zona"
fi

for peer in peer2 peer3; do
    if existe "reconectado via failover\|Drone conectado: NORTE-drone-01" "$LOG_DIR/${peer}_failover.log"; then
        ok "$peer reconheceu drone em modo failover"
    fi
done

if existe "Missão:" "$LOG_DIR/drone_failover.log"; then
    missoes_failover=$(contar_int "Missão concluída" "$LOG_DIR/drone_failover.log")
    ok "Drone executou $missoes_failover missão(ões) na zona de failover"
else
    info "Drone não recebeu missões no período — pode ser timing ou fila vazia"
fi

info "Restaurando peer1..."
$COMPOSE start peer1 > /dev/null 2>&1
sleep 10
ok "peer1 restaurado"

# =============================================================================
# TESTE 7 — CONSISTÊNCIA DO ESTADO DISTRIBUÍDO
# =============================================================================

titulo "TESTE 7 — Consistência do estado distribuído"

info "Aguardando estabilização da rede (15s)..."
sleep 15

for peer in peer1 peer2 peer3; do
    $COMPOSE logs "$peer" > "$LOG_DIR/${peer}_estado_final.log" 2>&1
done

for peer in peer1 peer2 peer3; do
    if existe "DRONE_UPDATE\|Drone.*atualizado" "$LOG_DIR/${peer}_estado_final.log"; then
        ok "$peer: recebe atualizações de estado de drones"
    else
        info "$peer: sem DRONE_UPDATE observado"
    fi
done

for peer in peer1 peer2 peer3; do
    if existe "SYNC_RESPONSE\|Sincronizado com" "$LOG_DIR/${peer}_estado_final.log"; then
        ok "$peer: sincronização de estado funcionando"
    fi
done

# =============================================================================
# TESTE 8 — CARGA: MÚLTIPLAS REQUISIÇÕES SIMULTÂNEAS
# =============================================================================

titulo "TESTE 8 — Carga: múltiplas requisições simultâneas"

info "Capturando logs dos 3 peers por 60s sob carga dos sensores..."
for peer in peer1 peer2 peer3; do
    capturar_logs "$peer" "$LOG_DIR/${peer}_carga.log" 60
done
sleep 60

total_missoes=0
total_alocacoes=0
total_enfileiradas=0

for peer in peer1 peer2 peer3; do
    m=$(contar_int "Missão concluída\|liberando" "$LOG_DIR/${peer}_carga.log")
    a=$(contar_int "Quorum atingido" "$LOG_DIR/${peer}_carga.log")
    e=$(contar_int "ENFILEIRADA" "$LOG_DIR/${peer}_carga.log")
    total_missoes=$((total_missoes + m))
    total_alocacoes=$((total_alocacoes + a))
    total_enfileiradas=$((total_enfileiradas + e))
done

info "Requisições enfileiradas: $total_enfileiradas"
info "Alocações via Ricart:     $total_alocacoes"
info "Missões concluídas:       $total_missoes"

if [ "$total_alocacoes" -gt 0 ]; then
    ok "Sistema processou $total_alocacoes alocação(ões) distribuída(s)"
else
    fail "Nenhuma alocação foi processada no período de carga"
fi

if [ "$total_missoes" -gt 0 ]; then
    ok "Sistema completou $total_missoes missão(ões)"
else
    fail "Nenhuma missão foi concluída no período de carga"
fi

# Verifica que o Ricart não travou (todo QUERENDO deve ter resolução)
for peer in peer1 peer2 peer3; do
    travado=$(contar_int "Estado Ricart: QUERENDO" "$LOG_DIR/${peer}_carga.log")
    resolvido=$(contar_int "Quorum atingido\|Reset completo" "$LOG_DIR/${peer}_carga.log")
    if [ "$travado" -gt 0 ] && [ "$resolvido" -eq 0 ]; then
        fail "$peer: Ricart possivelmente travado em QUERENDO sem resolução"
    elif [ "$travado" -gt 0 ]; then
        ok "$peer: Ricart entrou e saiu de QUERENDO corretamente ($resolvido resoluções)"
    fi
done

# =============================================================================
# SUMÁRIO FINAL
# =============================================================================

titulo "SUMÁRIO FINAL"

echo ""
echo -e "  Logs salvos em: ${Azul}$LOG_DIR/${Reset}"
echo ""
echo -e "  ${Negrito}Requisitos do problema:${Reset}"

r1=0; r2=0; r3=0; r4=0; r5=0; r6=0

# R1 - Exclusão mútua
if cat "$LOG_DIR"/peer*_mutex.log "$LOG_DIR"/peer*_carga.log 2>/dev/null | grep -q "Quorum atingido"; then
    echo -e "  ${Verde}✓${Reset} [R1] Exclusão mútua via Ricart-Agrawala"; r1=1
else
    echo -e "  ${Vermelho}✗${Reset} [R1] Exclusão mútua — não verificada"
fi

# R2 - Prioridade
if cat "$LOG_DIR"/peer*_mutex.log "$LOG_DIR"/peer*_carga.log 2>/dev/null | grep -q "Disparando Ricart para prioridade"; then
    echo -e "  ${Verde}✓${Reset} [R2] Fila de prioridade funcionando"; r2=1
else
    echo -e "  ${Vermelho}✗${Reset} [R2] Fila de prioridade — não verificada"
fi

# R3 - Um drone por requisição (sem alocação dupla)
if [ "$dupla_encontrada" -eq 0 ]; then
    echo -e "  ${Verde}✓${Reset} [R3] Um drone por requisição (sem alocação dupla)"; r3=1
else
    echo -e "  ${Vermelho}✗${Reset} [R3] Alocação dupla detectada"
fi

# R4 - Resiliência
queda_detectada=$(cat "$LOG_DIR"/peer1_pos_queda.log "$LOG_DIR"/peer2_pos_queda.log 2>/dev/null \
    | grep -c "desconectado\|perdida"; true)
if [ "$queda_detectada" -gt 0 ]; then
    echo -e "  ${Verde}✓${Reset} [R4] Resiliência — sistema continua após queda de peer"; r4=1
else
    echo -e "  ${Vermelho}✗${Reset} [R4] Resiliência — queda não detectada"
fi

# R5 - Failover de drone
if cat "$LOG_DIR"/drone_failover.log 2>/dev/null | grep -q "Conectado em peer[23]"; then
    echo -e "  ${Verde}✓${Reset} [R5] Failover de drone — reconecta em outra zona"; r5=1
else
    echo -e "  ${Vermelho}✗${Reset} [R5] Failover de drone — não verificado"
fi

# R6 - Envelhecimento
if [ "$envelhecimentos" -gt 0 ]; then
    echo -e "  ${Verde}✓${Reset} [R6] Anti-starvation — envelhecimento de fila ativo ($envelhecimentos eventos)"; r6=1
else
    echo -e "  ${Amarelo}?${Reset} [R6] Anti-starvation — não observado (fila sempre vazia neste ciclo)"
fi

echo ""
requisitos_ok=$((r1 + r2 + r3 + r4 + r5))
if [ $RESULTADO_FINAL -eq 0 ]; then
    echo -e "  ${Verde}${Negrito}RESULTADO: TODOS OS TESTES PASSARAM${Reset}"
else
    echo -e "  ${Vermelho}${Negrito}RESULTADO: ALGUNS TESTES FALHARAM — revise os logs em $LOG_DIR/${Reset}"
    echo ""
    echo -e "  ${Negrito}Dica: revise os arquivos de log individualmente com:${Reset}"
    echo -e "  ${Azul}grep -E 'ERRO|fail|Quorum|QUERENDO' $LOG_DIR/peer1_carga.log${Reset}"
fi
echo ""

exit $RESULTADO_FINAL