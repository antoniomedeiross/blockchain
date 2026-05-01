package repo

import (
	"container/heap"
	"log"
	"net"
	"pbl-2/zona/models"
	"pbl-2/zona/ricart"
	"sync"
)

// --- Heap de prioridade para requisições ---

type FilaHeap []models.Requisicao

func (f FilaHeap) Len() int { return len(f) }

// Maior prioridade sai primeiro (max-heap)
func (f FilaHeap) Less(i, j int) bool { return f[i].Prioridade > f[j].Prioridade }

func (f FilaHeap) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

func (f *FilaHeap) Push(x any) {
	*f = append(*f, x.(models.Requisicao))
}

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

// ALGORITMO RICART
var RicartInstance *ricart.Ricart

// DRONES ------------------------------------------------------------
var DroneMutex sync.RWMutex
var Drones = make(map[string]models.Drone)

func AtualizarDrone(d models.Drone) {
	DroneMutex.Lock()
	defer DroneMutex.Unlock()
	Drones[d.ID] = d
}

func BuscarDrones() map[string]models.Drone {
	DroneMutex.RLock()
	defer DroneMutex.RUnlock()
	copia := make(map[string]models.Drone)
	for k, v := range Drones {
		copia[k] = v
	}
	return copia
}

// FUNCAO PARA SELECIONAR UM DRONE LIVRE (USADA NA HMI)
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

// DRONES
// repo/drones.go — adiciona
var DroneConns = make(map[string]net.Conn) // droneID
var DroneConnMutex sync.RWMutex

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
}

func Enfileirar(req models.Requisicao) {
	FilaMutex.Lock()
	heap.Push(RequisicoesPendentes, req)
	FilaMutex.Unlock()
}

func ProximaRequisicao() (models.Requisicao, bool) {
	FilaMutex.Lock()
	defer FilaMutex.Unlock()
	if RequisicoesPendentes.Len() == 0 {
		return models.Requisicao{}, false
	}
	req := heap.Pop(RequisicoesPendentes).(models.Requisicao)
	return req, true
}

func TentarAlocarDaFila() {
	// 1. Verifica se o Ricart está livre. Se estiver ocupado, não faz nada agora.
	RicartInstance.Mu.Lock()
	estado := RicartInstance.Estado
	RicartInstance.Mu.Unlock()

	log.Printf("[DEBUG-FILA] Acionado! Estado Ricart: %s | Tamanho da Fila: %d\n", estado, RequisicoesPendentes.Len())

	if estado != models.EstadoLivre {
		return
	}

	// 2. Verifica se tem drone livre localmente
	drone, ok := SelecionarDroneLivre()
	if !ok {
		return
	}

	// 3. Pega a requisição mais importante da fila
	req, temReq := ProximaRequisicao()
	if !temReq {
		return
	}

	// 4. Inicia o pedido!
	log.Printf("[FILA] Disparando Ricart para prioridade %d (%s)", req.Prioridade, req.Ocorrencia)
	RicartInstance.IniciarRequisicao(drone.ID, req)
	log.Printf("[DEBUG-FILA] Sucesso! Iniciando Ricart para o drone %s\n", drone.ID)
}

//