package repo

import (
	"container/heap"
	"encoding/json"
	"log"
	"net"
	"pbl-2/zona/ledger"
	"pbl-2/zona/models"
	"pbl-2/zona/ricart"
	"sync"
	"time"
)

// --- Heap de prioridade para requisições ----------------------------

// FilaHeap é um heap de Requisicao, ordenado por prioridade e timestamp para desempate.
type FilaHeap []models.Requisicao

// Len, Less e Swap são necessários para implementar heap.Interface
func (f FilaHeap) Len() int { return len(f) }

// Maior prioridade sai primeiro (max-heap)
func (f FilaHeap) Less(i, j int) bool {
	if f[i].Prioridade == f[j].Prioridade {
		return f[i].Timestamp.Before(f[j].Timestamp)
	}
	return f[i].Prioridade > f[j].Prioridade
}

// Swap e Push/Pop são necessários para implementar heap.Interface
func (f FilaHeap) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

// Push e Pop usam ponteiros porque modificam o slice
func (f *FilaHeap) Push(x any) {
	*f = append(*f, x.(models.Requisicao))
}

// Pop remove e retorna o último elemento (o de maior prioridade)
func (f *FilaHeap) Pop() any {
	old := *f
	n := len(old)
	item := old[n-1]
	*f = old[:n-1]
	return item
}

// PEARS -------------------------------------------------------------
var Mutex sync.RWMutex

var Peers = make(map[string]models.Peer)

// ALGORITMO RICART --------------------------------------------------
var RicartInstance *ricart.Ricart

// DRONES ------------------------------------------------------------
var DroneMutex sync.RWMutex
var Drones = make(map[string]models.Drone)

func AtualizarDrone(d models.Drone) {
	DroneMutex.Lock()
	defer DroneMutex.Unlock()
	Drones[d.ID] = d
}


// AtualizarDroneRemoto é chamado quando a atualização vem de outro peer via DRONE_UPDATE.
func AtualizarDroneRemoto(d models.Drone) {
	// Se o Ricart está ativo, precisamos verificar se essa atualização impacta nossa requisição atual
	if RicartInstance != nil {
		RicartInstance.Mu.Lock()
		estado := RicartInstance.Estado
		droneAlvo := RicartInstance.DroneAlvo
		RicartInstance.Mu.Unlock()

		// Estamos NA_SECAO usando este drone — ignoramos update externo
		if estado == models.EstadoNaSecao && droneAlvo == d.ID {
			return
		}

		// Estamos QUERENDO este drone e outro peer já o marcou como ocupado —
		// perdemos a corrida: aceita o update e aborta o Ricart localmente
		if estado == models.EstadoQuerendo && droneAlvo == d.ID && d.Status == models.StatusOcupado {
			log.Printf("[RICART] ⚠ DRONE_UPDATE: %s já está ocupado por outro peer — abortando requisição local\n", d.ID)

			RicartInstance.Mu.Lock()
			if RicartInstance.Estado == models.EstadoQuerendo && RicartInstance.DroneAlvo == d.ID {
				RicartInstance.Abortando = true
			}

			RicartInstance.Mu.Unlock()
			DroneMutex.Lock()
			Drones[d.ID] = d
			DroneMutex.Unlock()

			go RicartInstance.AbortarRequisicao(d.ID)
			return
		}
	}

	// Aceita a atualização normalmente
	DroneMutex.Lock()
	Drones[d.ID] = d
	DroneMutex.Unlock()

	// Se o drone ficou livre, pode haver requisições na fila esperando por ele
	if d.Status == models.StatusLivre {
		go func() {
			TentarAlocarDaFila()
		}()
	}
}


// busca uma cópia do mapa de drones para enviar ou printar
func BuscarDrones() map[string]models.Drone {
	DroneMutex.RLock()
	defer DroneMutex.RUnlock()
	copia := make(map[string]models.Drone)
	for k, v := range Drones {
		copia[k] = v
	}
	return copia
}


// FUNCAO PARA SELECIONAR UM DRONE LIVRE CHAMADO QUANDO VAI INICIAR O RICART
func SelecionarDroneLivre() (models.Drone, bool) {
	DroneMutex.RLock()
	defer DroneMutex.RUnlock()

	for _, drone := range Drones {
		if drone.Status == models.StatusLivre { // offline não é livre
			return drone, true
		}
	}
	return models.Drone{}, false
}


// DRONES -------------------------------------------------------------

var DroneConns = make(map[string]net.Conn) // droneID -> conexão TCP
var DroneConnMutex sync.RWMutex // protege o mapa de conexões dos drones

// DronesGerenciados rastreia quais drones foram alocados via Ricart por esta zona.
// Um drone entra aqui quando AoAlocar é chamado e sai quando Liberar é chamado.
var DronesGerenciados = make(map[string]bool)
var DronesGeridosMutex sync.RWMutex

func RegistrarGerenciamento(droneID string) {
	DronesGeridosMutex.Lock()
	DronesGerenciados[droneID] = true
	DronesGeridosMutex.Unlock()
}

func RemoverGerenciamento(droneID string) {
	DronesGeridosMutex.Lock()
	delete(DronesGerenciados, droneID)
	DronesGeridosMutex.Unlock()
}

func ListarGerenciados() []string {
	DronesGeridosMutex.RLock()
	defer DronesGeridosMutex.RUnlock()
	lista := make([]string, 0, len(DronesGerenciados))
	for id := range DronesGerenciados {
		lista = append(lista, id)
	}
	return lista
}

// DronesServindo rastreia qual zona solicitou a missão atual de um drone conectado aqui.
// droneID -> zonaSolicitante
var DronesServindo = make(map[string]string)
var DronesServindoMutex sync.RWMutex

func RegistrarServico(droneID string, zonaSolicitante string) {
	DronesServindoMutex.Lock()
	DronesServindo[droneID] = zonaSolicitante
	DronesServindoMutex.Unlock()
}

func ObterSolicitante(droneID string) string {
	DronesServindoMutex.RLock()
	defer DronesServindoMutex.RUnlock()
	return DronesServindo[droneID]
}

func FinalizarServico(droneID string) {
	DronesServindoMutex.Lock()
	delete(DronesServindo, droneID)
	DronesServindoMutex.Unlock()
}

func EstaSendoGerenciado(droneID string) bool {
	DronesGeridosMutex.RLock()
	defer DronesGeridosMutex.RUnlock()
	return DronesGerenciados[droneID]
}

// repo/drones.go
var BroadcastFn func(drone models.Drone)

func RegistrarConexaoDrone(droneID string, conn net.Conn) {
	DroneConnMutex.Lock()
	defer DroneConnMutex.Unlock()
	DroneConns[droneID] = conn
}

func RemoverConexaoDrone(droneID string) {
	DroneConnMutex.Lock()
	defer DroneConnMutex.Unlock()
	delete(DroneConns, droneID)
}

func EnviarParaDrone(droneID string, msg []byte) bool {
	DroneConnMutex.RLock()
	conn, ok := DroneConns[droneID]
	DroneConnMutex.RUnlock()
	if !ok || conn == nil {
		return false
	}
	_, err := conn.Write(append(msg, '\n'))

	
	return err == nil
}

// Fila de prioridade (heap)
var RequisicoesPendentes = &FilaHeap{}
var FilaMutex sync.Mutex

func init() {
	heap.Init(RequisicoesPendentes)
	go envelhecerFila()
}

// envelhecerFila incrementa a prioridade de todas as requisições pendentes
// a cada 10 segundos, até o máximo de 5
func envelhecerFila() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		FilaMutex.Lock()
		alterou := false
		for i, req := range *RequisicoesPendentes {
			if req.Prioridade < 5 {
				(*RequisicoesPendentes)[i].Prioridade++
				alterou = true
			}
		}
		if alterou {
			heap.Init(RequisicoesPendentes) // reordena o heap após mudanças
			log.Printf("[FILA] ⏫ Envelhecimento: %d req(s) subiram de prioridade\n", RequisicoesPendentes.Len())
		}
		FilaMutex.Unlock()
	}
}

// Enfileirar adiciona uma nova requisição à fila de prioridade.
func Enfileirar(req models.Requisicao) {
	FilaMutex.Lock()
	heap.Push(RequisicoesPendentes, req)
	FilaMutex.Unlock()
}

// NotificarMissaoConcluida envia MISSAO_CONCLUIDA_ACK para a zona base de um drone
// de failover, para que ela libere o Ricart corretamente.
func NotificarMissaoConcluida(zonaBase string, droneID string) {
	Mutex.RLock()
	peer, exists := Peers[zonaBase]
	Mutex.RUnlock()

	if !exists || !peer.Alive || peer.Conn == nil {
		// Zona base está offline — libera localmente para não travar
		log.Printf("[FAILOVER] ⚠ Zona base %s offline — Ricart liberado localmente para drone %s\n", zonaBase, droneID)
		RicartInstance.Liberar(droneID)
		return
	}

	ack := models.AckMissao{DroneID: droneID}
	msg := models.Mensagem{
		Tipo:  "MISSAO_CONCLUIDA_ACK",
		Dados: ack,
	}
	data, _ := json.Marshal(msg)
	peer.Conn.Write(append(data, '\n'))
}

// ProximaRequisicao retorna a próxima requisição da fila, ou false se a fila estiver vazia.
func ProximaRequisicao() (models.Requisicao, bool) {
	FilaMutex.Lock()
	defer FilaMutex.Unlock()
	if RequisicoesPendentes.Len() == 0 {
		return models.Requisicao{}, false
	}
	req := heap.Pop(RequisicoesPendentes).(models.Requisicao)
	return req, true
}

// TentarAlocarDaFila verifica se tem requisições pendentes e drones livres, e tenta alocar o drone para a requisição mais prioritária.
func TentarAlocarDaFila() {
	// 1. Verifica o estado do Ricart.
	RicartInstance.Mu.Lock()
	estado := RicartInstance.Estado
	reqAtual := RicartInstance.RequisicaoAtual
	droneAlvo := RicartInstance.DroneAlvo
	RicartInstance.Mu.Unlock()

	if RequisicoesPendentes.Len() > 0 {
		log.Printf("[FILA] Estado=%s | Pendentes=%d\n", estado, RequisicoesPendentes.Len())
	}

	if estado == models.EstadoQuerendo && reqAtual != nil {
		FilaMutex.Lock()
		if RequisicoesPendentes.Len() > 0 {
			maiorReq := (*RequisicoesPendentes)[0]
			if maiorReq.Prioridade > reqAtual.Prioridade {
				FilaMutex.Unlock()
				log.Printf("[FILA] ⚠ Preempção local: req fila (prior=%d) > req atual (prior=%d). Abortando req atual.\n", maiorReq.Prioridade, reqAtual.Prioridade)
				RicartInstance.AbortarRequisicao(droneAlvo)
				return // AbortarRequisicao vai chamar TentarAlocarDaFila de novo no final
			}
		}
		FilaMutex.Unlock()
	}

	if estado != models.EstadoLivre {
		return
	}

	// 2. Verifica se tem drone livre localmente
	drone, ok := SelecionarDroneLivre()
	if !ok {
		return
	}

	// 3. Pega a requisição mais importante da fila (espiamos sem remover ainda)
	FilaMutex.Lock()
	if RequisicoesPendentes.Len() == 0 {
		FilaMutex.Unlock()
		return
	}
	proximaReq := (*RequisicoesPendentes)[0]
	FilaMutex.Unlock()

	// 3.5 Verifica validade e saldo da requisição no topo da fila.
	// Requisições inválidas (zona vazia) ou sem saldo são descartadas imediatamente
	// para não bloquear requisições legítimas atrás delas.
	if proximaReq.ZonaID == "" || proximaReq.Sensor == "" {
		FilaMutex.Lock()
		heap.Pop(RequisicoesPendentes)
		FilaMutex.Unlock()
		log.Printf("[FILA] ⚠ Requisição inválida removida da fila (zona='%s')\n", proximaReq.ZonaID)
		go TentarAlocarDaFila()
		return
	}

	if !ledger.Instancia.TemSaldo(proximaReq.ZonaID) {
		log.Printf("[FILA] ✗ Zona %s sem saldo — requisição descartada\n", proximaReq.ZonaID)
		FilaMutex.Lock()
		heap.Pop(RequisicoesPendentes)
		FilaMutex.Unlock()
		go TentarAlocarDaFila()
		return
	}

	// 4. Se tem saldo e drone, agora sim removemos e iniciamos
	req, _ := ProximaRequisicao()

	log.Printf("[FILA] ► Disparando Ricart — prioridade=%d (%s)\n", req.Prioridade, req.Ocorrencia)
	RicartInstance.IniciarRequisicao(drone.ID, req)
	log.Printf("[FILA] ✔ Drone selecionado: %s\n", drone.ID)
}

