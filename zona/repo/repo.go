package repo

import (
	"container/heap"
	"encoding/json"
	"log"
	"net"
	"pbl-2/zona/models"
	"pbl-2/zona/ricart"
	"sync"
	"time"
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

// AtualizarDroneRemoto é chamado quando a atualização vem de outro peer via DRONE_UPDATE.
//
// Regra de autoridade:
//   - Se o Ricart LOCAL está NA_SEÇÃO para este drone, somos nós que estamos usando —
//     ignoramos qualquer update externo para não corromper nosso estado.
//   - Se o Ricart LOCAL está QUERENDO e chega um update de "ocupado" para o mesmo drone,
//     significa que PERDEMOS a corrida — aceitamos o update e abortamos nossa requisição.
//   - Em todos os outros casos aceitamos o update normalmente.
func AtualizarDroneRemoto(d models.Drone) {
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
			// CRÍTICO: seta Abortando=true DENTRO do lock do Ricart antes de soltar,
			// para que ReceberRelease/watchdog que adquirirem o lock logo depois
			// já vejam o flag e não acionem AoAlocar.
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
// a cada 10 segundos, até o máximo de 5, para evitar starvation.
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

	// 3. Pega a requisição mais importante da fila
	req, temReq := ProximaRequisicao()
	if !temReq {
		return
	}

	// 4. Inicia o pedido!
	log.Printf("[FILA] ► Disparando Ricart — prioridade=%d (%s)\n", req.Prioridade, req.Ocorrencia)
	RicartInstance.IniciarRequisicao(drone.ID, req)
	log.Printf("[FILA] ✔ Drone selecionado: %s\n", drone.ID)
}

//