// teste/main.go
// Teste de integração interativo do sistema distribuído de drones.
//
// Uso:
//
//	go run teste/main.go -peer1=localhost:9090 -peer2=localhost:9091 -peer3=localhost:9092
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── Protocolo ────────────────────────────────────────────────────────────────

type Requisicao struct {
	Tipo       string    `json:"tipo"`
	Sensor     string    `json:"sensor"`
	Zona       string    `json:"zona"`
	Ocorrencia string    `json:"ocorrencia"`
	Prioridade int       `json:"prioridade"`
	Timestamp  time.Time `json:"timestamp"`
}

type Mensagem struct {
	Tipo      string      `json:"tipo"`
	De        string      `json:"de"`
	Para      string      `json:"para"`
	Dados     interface{} `json:"dados"`
	Timestamp time.Time   `json:"timestamp"`
}

type ResultadoReq struct {
	SensorID   string
	Peer       string
	Ocorrencia string
	Prioridade int
	EnviadoEm  time.Time
	Latencia   time.Duration
	Erro       error
}

// ── Métricas ─────────────────────────────────────────────────────────────────

type Metricas struct {
	mu        sync.Mutex
	latencias []time.Duration
	erros     int
	inicio    time.Time
}

func NovasMetricas() *Metricas {
	return &Metricas{inicio: time.Now()}
}

func (m *Metricas) Registrar(r ResultadoReq) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.Erro != nil {
		m.erros++
		return
	}
	m.latencias = append(m.latencias, r.Latencia)
}

func (m *Metricas) Relatorio(tit string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	duracao := time.Since(m.inicio)
	sucessos := len(m.latencias)
	total := sucessos + m.erros

	fmt.Printf("\n%s%s┌─────────────────────────────────────────────────────┐%s\n", Negrito, Ciano, Reset)
	fmt.Printf("%s%s│%s  📊 MÉTRICAS — %-37s%s%s│%s\n", Negrito, Ciano, Reset, tit, Negrito, Ciano, Reset)
	fmt.Printf("%s%s├─────────────────────────────────────────────────────┤%s\n", Negrito, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "Duração total:", Negrito, duracao.Round(time.Millisecond), Reset, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "Requisições enviadas:", Negrito,
		fmt.Sprintf("%d / %d  erros: %d", sucessos, total, m.erros), Reset, Ciano, Reset)

	throughput := 0.0
	if duracao.Seconds() > 0 {
		throughput = float64(sucessos) / duracao.Seconds()
	}
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "Throughput:", Negrito,
		fmt.Sprintf("%.2f req/s", throughput), Reset, Ciano, Reset)

	if sucessos == 0 {
		fmt.Printf("%s%s│%s  (nenhuma requisição bem-sucedida)                   %s%s│%s\n", Negrito, Ciano, Reset, Ciano, Negrito, Reset)
		fmt.Printf("%s%s└─────────────────────────────────────────────────────┘%s\n\n", Negrito, Ciano, Reset)
		return
	}

	sort.Slice(m.latencias, func(i, j int) bool { return m.latencias[i] < m.latencias[j] })
	minL := m.latencias[0]
	maxL := m.latencias[sucessos-1]
	p50 := m.latencias[int(float64(sucessos)*0.50)]
	p90 := m.latencias[int(math.Min(float64(sucessos-1), float64(sucessos)*0.90))]
	p99 := m.latencias[int(math.Min(float64(sucessos-1), float64(sucessos)*0.99))]
	var soma time.Duration
	for _, l := range m.latencias {
		soma += l
	}
	media := soma / time.Duration(sucessos)

	fmt.Printf("%s%s├─────────────────────────────────────────────────────┤%s\n", Negrito, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "Latência mín:", Negrito, minL.Round(time.Microsecond), Reset, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "Latência média:", Negrito, media.Round(time.Microsecond), Reset, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "Latência máx:", Negrito, maxL.Round(time.Microsecond), Reset, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "p50 (mediana):", Negrito, p50.Round(time.Microsecond), Reset, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "p90:", Negrito, p90.Round(time.Microsecond), Reset, Ciano, Reset)
	fmt.Printf("%s%s│%s  %-28s %s%-22s%s%s│%s\n", Negrito, Ciano, Reset, "p99:", Negrito, p99.Round(time.Microsecond), Reset, Ciano, Reset)

	// Histograma
	fmt.Printf("%s%s├─────────────────────────────────────────────────────┤%s\n", Negrito, Ciano, Reset)
	fmt.Printf("%s%s│%s  Histograma de latência:                            %s%s│%s\n", Negrito, Ciano, Reset, Ciano, Negrito, Reset)
	buckets := []struct {
		label string
		max   time.Duration
	}{
		{"< 1ms  ", 1 * time.Millisecond},
		{"1–5ms  ", 5 * time.Millisecond},
		{"5–20ms ", 20 * time.Millisecond},
		{"20–50ms", 50 * time.Millisecond},
		{"> 50ms ", time.Duration(math.MaxInt64)},
	}
	contagens := make([]int, len(buckets))
	for _, l := range m.latencias {
		for i, b := range buckets {
			if l < b.max {
				contagens[i]++
				break
			}
		}
	}
	barMax := 20
	for i, b := range buckets {
		barLen := 0
		if sucessos > 0 {
			barLen = int(float64(contagens[i]) / float64(sucessos) * float64(barMax))
		}
		bar := strings.Repeat("█", barLen) + strings.Repeat("░", barMax-barLen)
		fmt.Printf("%s%s│%s  %s %s%s%s %3d req              %s%s│%s\n",
			Negrito, Ciano, Reset,
			b.label, Verde, bar, Reset,
			contagens[i], Ciano, Negrito, Reset)
	}
	fmt.Printf("%s%s└─────────────────────────────────────────────────────┘%s\n\n", Negrito, Ciano, Reset)
}

// ── Cores ─────────────────────────────────────────────────────────────────────

const (
	Verde    = "\033[0;32m"
	Vermelho = "\033[0;31m"
	Amarelo  = "\033[1;33m"
	Azul     = "\033[0;34m"
	Ciano    = "\033[0;36m"
	Magenta  = "\033[0;35m"
	Negrito  = "\033[1m"
	Reset    = "\033[0m"
)

func ok(msg string)    { fmt.Printf("  %s✓%s %s\n", Verde, Reset, msg) }
func fail(msg string)  { fmt.Printf("  %s✗%s %s\n", Vermelho, Reset, msg) }
func info(msg string)  { fmt.Printf("  %s→%s %s\n", Azul, Reset, msg) }
func aviso(msg string) { fmt.Printf("  %s!%s %s\n", Amarelo, Reset, msg) }

func titulo(msg string) {
	fmt.Printf("\n%s%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n", Negrito, Amarelo, Reset)
	fmt.Printf("%s%s  %s%s\n", Negrito, Amarelo, msg, Reset)
	fmt.Printf("%s%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n", Negrito, Amarelo, Reset)
}

func separador() {
	fmt.Printf("  %s%s──────────────────────────────────────────────────%s\n", Ciano, Negrito, Reset)
}

// ── Variáveis de ambiente ──────────────────────────────────────────────────────

var (
	addrPeer1 string
	addrPeer2 string
	addrPeer3 string
)

func carregarEnv(nome, padrao string) string {
	valor := os.Getenv(nome)
	if valor == "" {
		return padrao
	}
	return valor
}

// ── Comunicação TCP ───────────────────────────────────────────────────────────

func enviarRequisicao(peerAddr, sensorID, zona, ocorrencia string, prioridade int) ResultadoReq {
	res := ResultadoReq{
		SensorID:   sensorID,
		Peer:       peerAddr,
		Ocorrencia: ocorrencia,
		Prioridade: prioridade,
		EnviadoEm:  time.Now(),
	}

	inicio := time.Now()

	conn, err := net.DialTimeout("tcp", peerAddr, 5*time.Second)
	if err != nil {
		res.Erro = fmt.Errorf("conexão falhou: %w", err)
		return res
	}
	defer conn.Close()

	fmt.Fprintf(conn, "IAM:SENSOR:%s\n", sensorID)
	leitor := bufio.NewReader(conn)
	resp, err := leitor.ReadString('\n')
	if err != nil || strings.TrimSpace(resp) != "OK" {
		res.Erro = fmt.Errorf("handshake falhou: resp=%q err=%v", resp, err)
		return res
	}

	req := Requisicao{
		Tipo:       "REQUISICAO_DRONE",
		Sensor:     sensorID,
		Zona:       zona,
		Ocorrencia: ocorrencia,
		Prioridade: prioridade,
		Timestamp:  time.Now(),
	}
	msg := Mensagem{
		Tipo:      "REQUISICAO_DRONE",
		De:        sensorID,
		Para:      "",
		Dados:     req,
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(msg)
	conn.Write(append(data, '\n'))

	res.Latencia = time.Since(inicio)
	return res
}

func peerAcessivel(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// ── Verificação de peers ──────────────────────────────────────────────────────

func verificarPeers() bool {
	titulo("Verificando conectividade com os peers")
	peers := []struct{ zona, addr string }{
		{"NORTE (peer1)", addrPeer1},
		{"SUL   (peer2)", addrPeer2},
		{"LESTE (peer3)", addrPeer3},
	}
	todos := true
	for _, p := range peers {
		if peerAcessivel(p.addr) {
			ok(fmt.Sprintf("Peer %s [%s] acessível", p.zona, p.addr))
		} else {
			fail(fmt.Sprintf("Peer %s [%s] INACESSÍVEL", p.zona, p.addr))
			todos = false
		}
	}
	if !todos {
		fmt.Printf("\n  %sERRO: Suba o sistema antes de rodar os testes (docker compose up -d).%s\n", Vermelho, Reset)
	}
	return todos
}

// ── TESTES ────────────────────────────────────────────────────────────────────

func teste1Prioridade() {
	titulo("TESTE 1 — Fila de prioridade: 5 requisições com prioridades diferentes")
	info("Enviando 5 requisições ao peer1 (NORTE) com prioridades fora de ordem")
	info("Esperado nos logs: despacho na ordem 5 → 4 → 2 → 1 → 0")
	separador()

	casos := []struct {
		ocorrencia string
		prioridade int
	}{
		{"falha_sinalizacao", 5},
		{"congestionamento_corredor", 2},
		{"suspeita_bloqueio_rota", 3},
		{"embarcacao_deriva", 1},
		{"inspecao_visual_urgente", 4},
	}

	met := NovasMetricas()
	for i, c := range casos {
		sensorID := fmt.Sprintf("sensor-prio-%d", i+1)
		r := enviarRequisicao(addrPeer1, sensorID, "NORTE", c.ocorrencia, c.prioridade)
		met.Registrar(r)
		if r.Erro != nil {
			fail(fmt.Sprintf("prioridade=%d — ERRO: %v", c.prioridade, r.Erro))
		} else {
			ok(fmt.Sprintf("Enfileirado: %sprioridade=%d%s — %s  %s[latência: %s]%s",
				Negrito, c.prioridade, Reset, c.ocorrencia,
				Ciano, r.Latencia.Round(time.Microsecond), Reset))
		}
		time.Sleep(100 * time.Millisecond)
	}

	met.Relatorio("Prioridade")
	separador()
	aviso("Observe os logs do peer1:")
	fmt.Printf("  %sdocker compose logs -f peer1%s\n", Ciano, Reset)
	info("Procure por: [FILA] Disparando Ricart para prioridade X")
	info("A ordem deve ser: 5 → 4 → 2 → 1 → 0")
}

func teste2Simultaneo() {
	titulo("TESTE 2 — Exclusão mútua: 2 peers requisitam ao mesmo tempo")
	info("Enviando requisições simultâneas do peer1 (NORTE) e peer2 (SUL)")
	info("Esperado: apenas UM ganha o quorum Ricart; o outro espera na fila")
	separador()

	type cfg struct {
		addr, zona, sensor, ocorrencia string
		prioridade                     int
	}
	simult := []cfg{
		{addrPeer1, "NORTE", "sensor-simult-norte", "objeto_nao_identificado", 3},
		{addrPeer2, "SUL", "sensor-simult-sul", "embarcacao_deriva", 3},
	}

	met := NovasMetricas()
	var wg sync.WaitGroup
	for _, s := range simult {
		wg.Add(1)
		go func(c cfg) {
			defer wg.Done()
			r := enviarRequisicao(c.addr, c.sensor, c.zona, c.ocorrencia, c.prioridade)
			met.Registrar(r)
			if r.Erro != nil {
				fail(fmt.Sprintf("%s — ERRO: %v", c.sensor, r.Erro))
			} else {
				ok(fmt.Sprintf("%s enviou às %s%s%s (prioridade=%d)  %s[latência: %s]%s",
					c.sensor, Negrito, r.EnviadoEm.Format("15:04:05.000"), Reset, c.prioridade,
					Ciano, r.Latencia.Round(time.Microsecond), Reset))
			}
		}(s)
	}
	wg.Wait()

	met.Relatorio("Exclusão Mútua")
	separador()
	aviso("Observe os logs dos dois peers:")
	fmt.Printf("  %sdocker compose logs -f peer1%s\n", Ciano, Reset)
	fmt.Printf("  %sdocker compose logs -f peer2%s\n", Ciano, Reset)
	info("Procure por: dois REQUEST quase simultâneos")
	info("Apenas 1 deve receber [RICART] Quorum atingido primeiro")
	info("O outro deve aparecer como [RICART] Adiando REPLY")
}

func teste3Distribuicao() {
	titulo("TESTE 3 — Distribuição: sensores em zonas diferentes requisitam drones")
	info("Enviando 1 requisição para cada zona (NORTE, SUL, LESTE) quase ao mesmo tempo")
	info("Esperado: cada zona enfileira; Ricart coordena entre os 3 peers")
	separador()

	type cfg struct {
		addr, zona, sensor, ocorrencia string
		prioridade                     int
	}
	zonas := []cfg{
		{addrPeer1, "NORTE", "sensor-dist-norte", "replanejamento_risco_ambiental", 5},
		{addrPeer1, "NORTE", "sensor-dist-norte", "replanejamento_risco_ambiental", 2},

		{addrPeer2, "SUL", "sensor-dist-sul", "falha_sinalizacao", 4},
		{addrPeer2, "SUL", "sensor-dist-sul", "replanejamento_risco_ambiental", 2},

		{addrPeer3, "LESTE", "sensor-dist-leste", "inspecao_visual_urgente", 2},
		{addrPeer3, "LESTE", "sensor-dist-leste", "replanejamento_risco_ambiental", 1},
	}

	met := NovasMetricas()
	var wg sync.WaitGroup
	for i, z := range zonas {
		wg.Add(1)
		go func(idx int, c cfg) {
			defer wg.Done()
			time.Sleep(time.Duration(idx*50) * time.Millisecond)
			r := enviarRequisicao(c.addr, c.sensor, c.zona, c.ocorrencia, c.prioridade)
			met.Registrar(r)
			if r.Erro != nil {
				fail(fmt.Sprintf("Zona %s — ERRO: %v", c.zona, r.Erro))
			} else {
				ok(fmt.Sprintf("Zona %s%s%s — prioridade=%d (%s)  %s[latência: %s]%s",
					Negrito, c.zona, Reset, c.prioridade, c.ocorrencia,
					Ciano, r.Latencia.Round(time.Microsecond), Reset))
			}
		}(i, z)
	}
	wg.Wait()

	met.Relatorio("Distribuição entre Zonas")
	separador()
}

func teste4Carga() {
	titulo("TESTE 4 — Carga: 10 requisições em rajada para o peer2 (SUL)")
	info("Enviando 10 requisições com prioridades variadas em ~1.5s")
	info("Esperado: todas enfileiradas, atendidas por prioridade, sem travamento do Ricart")
	separador()

	prios := []int{3, 0, 5, 2, 1, 4, 0, 5, 3, 2}
	ocorrencias := []string{
		"suspeita_bloqueio_rota", "falha_sinalizacao", "embarcacao_deriva",
		"congestionamento_corredor", "objeto_nao_identificado", "inspecao_visual_urgente",
		"replanejamento_risco_ambiental", "suspeita_bloqueio_rota", "embarcacao_deriva",
		"falha_sinalizacao",
	}

	met := NovasMetricas()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		enviadas int
	)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sensorID := fmt.Sprintf("sensor-carga-%02d", idx+1)
			r := enviarRequisicao(addrPeer1, sensorID, "NORTE", ocorrencias[idx], prios[idx])
			_ = enviarRequisicao(addrPeer2, sensorID, "SUL", ocorrencias[idx], prios[idx])
			_ = enviarRequisicao(addrPeer3, sensorID, "LESTE", ocorrencias[idx], prios[idx])
			met.Registrar(r)
			mu.Lock()
			defer mu.Unlock()
			if r.Erro != nil {
				fail(fmt.Sprintf("sensor-carga-%02d — ERRO: %v", idx+1, r.Erro))
			} else {
				fmt.Printf("  %s[%02d]%s prioridade=%s%d%s — %s  %s[latência: %s]%s\n",
					Ciano, idx+1, Reset, Negrito, prios[idx], Reset, ocorrencias[idx],
					Ciano, r.Latencia.Round(time.Microsecond), Reset)
				enviadas++
			}
		}(i)
		time.Sleep(150 * time.Millisecond)
	}
	wg.Wait()

	separador()
	if enviadas == 10 {
		ok("10/10 requisições enviadas com sucesso")
	} else {
		fail(fmt.Sprintf("%d/10 requisições enviadas", enviadas))
	}
	met.Relatorio("Carga (10 req em rajada — 3 peers)")
}

func teste5Empate() {
	titulo("TESTE 5 — Desempate: 3 requisições com mesma prioridade")
	info("Enviando prioridade=2 para NORTE, SUL e LESTE com intervalo de 500ms")
	info("Esperado: atendidas na ordem de chegada (timestamp mais antigo vence)")
	separador()

	empate := []struct {
		addr, zona, sensor, ocorrencia string
	}{
		{addrPeer2, "SUL", "sensor-empate-1", "embarcacao_deriva"},
		{addrPeer2, "SUL", "sensor-empate-2", "falha_sinalizacao"},
		{addrPeer2, "SUL", "sensor-empate-3", "objeto_nao_identificado"},
	}

	met := NovasMetricas()
	for i, e := range empate {
		r := enviarRequisicao(e.addr, e.sensor, e.zona, e.ocorrencia, 2)
		met.Registrar(r)
		if r.Erro != nil {
			fail(fmt.Sprintf("%s — ERRO: %v", e.sensor, r.Erro))
		} else {
			ok(fmt.Sprintf("#%d enviado às %s%s%s — Zona %s (prioridade=2) - %s  %s[latência: %s]%s",
				i+1, Negrito, r.EnviadoEm.Format("15:04:05.000"), Reset, e.zona, e.ocorrencia,
				Ciano, r.Latencia.Round(time.Microsecond), Reset))
		}
		time.Sleep(500 * time.Millisecond)
	}

	met.Relatorio("Desempate por Timestamp")
	separador()
	aviso("Observe os logs dos 3 peers:")
	info("O Ricart usa timestamp como desempate — a requisição mais antiga deve ganhar o quorum primeiro")
}

// ── MENU ──────────────────────────────────────────────────────────────────────

func exibirMenu() {
	fmt.Printf("\n%s%s╔══════════════════════════════════════════════════╗%s\n", Negrito, Azul, Reset)
	fmt.Printf("%s%s║       TESTES DO SISTEMA DISTRIBUÍDO DE DRONES    ║%s\n", Negrito, Azul, Reset)
	fmt.Printf("%s%s╠══════════════════════════════════════════════════╣%s\n", Negrito, Azul, Reset)
	fmt.Printf("%s%s║%s  %s1%s  Fila de prioridade (5 requisições distintas)   %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s║%s  %s2%s  Exclusão mútua (2 peers simultâneos)           %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s║%s  %s3%s  Distribuição entre zonas (3 peers)             %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s║%s  %s4%s  Carga (10 requisições em rajada)               %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s║%s  %s5%s  Desempate por timestamp (mesma prioridade)     %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s║%s  %s6%s  Rodar todos os testes em sequência             %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s║%s  %s0%s  Sair                                           %s║%s\n", Negrito, Azul, Reset, Negrito, Reset, Azul+Negrito, Reset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════╝%s\n", Negrito, Azul, Reset)
	fmt.Printf("\n  %sPeers:%s peer1=%s | peer2=%s | peer3=%s\n",
		Magenta, Reset, addrPeer1, addrPeer2, addrPeer3)
	fmt.Printf("\n  %sEscolha um teste:%s ", Negrito, Reset)
}

func aguardarEnter() {
	fmt.Printf("\n  %sPressione ENTER para voltar ao menu...%s ", Amarelo, Reset)
	bufio.NewReader(os.Stdin).ReadString('\n')
}

// ── MAIN ──────────────────────────────────────────────────────────────────────

func main() {
	addrPeer1 = carregarEnv("PEER1", "localhost:9090")
	addrPeer2 = carregarEnv("PEER2", "localhost:9091")
	addrPeer3 = carregarEnv("PEER3", "localhost:9092")

	if !verificarPeers() {
		return
	}

	scanner := bufio.NewReader(os.Stdin)

	for {
		exibirMenu()
		linha, _ := scanner.ReadString('\n')
		opcao := strings.TrimSpace(linha)

		switch opcao {
		case "1":
			teste1Prioridade()
			aguardarEnter()
		case "2":
			teste2Simultaneo()
			aguardarEnter()
		case "3":
			teste3Distribuicao()
			aguardarEnter()
		case "4":
			teste4Carga()
			aguardarEnter()
		case "5":
			teste5Empate()
			aguardarEnter()
		case "6":
			teste1Prioridade()
			time.Sleep(2 * time.Second)
			teste2Simultaneo()
			time.Sleep(2 * time.Second)
			teste3Distribuicao()
			time.Sleep(2 * time.Second)
			teste4Carga()
			time.Sleep(2 * time.Second)
			teste5Empate()
			aguardarEnter()
		case "0":
			fmt.Printf("\n  %sSaindo...%s\n\n", Amarelo, Reset)
			return
		default:
			fmt.Printf("\n  %sOpção inválida. Digite um número de 0 a 6.%s\n", Vermelho, Reset)
		}
	}
}