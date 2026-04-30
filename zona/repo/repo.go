package repo

import (
	"log"
	"net"
	"pbl-2/zona/models"
	"pbl-2/zona/ricart"
	"sync"
)

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

// lista prioridade
var RequisicoesPendentes []models.Requisicao
var FilaMutex sync.Mutex

func Enfileirar(req models.Requisicao) {
	FilaMutex.Lock()
	RequisicoesPendentes = append(RequisicoesPendentes, req)
	FilaMutex.Unlock()
}

func ProximaRequisicao() (models.Requisicao, bool) {
	FilaMutex.Lock()
	defer FilaMutex.Unlock()
	if len(RequisicoesPendentes) == 0 {
		return models.Requisicao{}, false
	}
	req := RequisicoesPendentes[0]
	RequisicoesPendentes = RequisicoesPendentes[1:]
	return req, true
}

func TentarAlocarDaFila() {
	// 1. Verifica se o Ricart está livre. Se estiver ocupado, não faz nada agora.
	RicartInstance.Mu.Lock()
	estado := RicartInstance.Estado
	RicartInstance.Mu.Unlock()

	log.Printf("[DEBUG-FILA] Acionado! Estado Ricart: %s | Tamanho da Fila: %d\n", estado, len(RequisicoesPendentes))

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
	RicartInstance.IniciarRequisicao(drone.ID, req)
}

//
