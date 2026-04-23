package models

import (
	"net"
	"sync"
	"time"
)

//STRUCT DO PEAR
type Peer struct {
	ZoneID   string // "NORTE", "SUL", "LESTE"
	Address  string // "172.20.0.3:5000"
	Alive    bool
	LastSeen time.Time // Heartbeat
	Conn     net.Conn  // Conexão com este peer (para enviar mensagens)
}


// STRUCT DO DRONEEEEEE

type DroneStatus string
//tipos de estados possiveis
const (
    StatusLivre   DroneStatus = "livre"
    StatusOcupado DroneStatus = "ocupado"
    StatusOffline DroneStatus = "offline"
)

type Drone struct {
    ID        string      `json:"id"`
    Status    DroneStatus `json:"status"`
    ZonaBase  string      `json:"zona_base"`
    ZonaAtual string      `json:"zona_atual"` // pode estar atendendo outra zona
}

//STRUCT do server
type Server struct {
    ID     string
    Peers  map[string]Peer
    Drones map[string]Drone  // estado local replicado
    mu     sync.RWMutex      // protege leitura/escrita concorrente
}

//STRUCT De update no drone
type DroneUpdate struct {
    Tipo       string `json:"tipo"` // "DRONE_UPDATE"
    DroneID    string `json:"drone_id"`
    NovoStatus string `json:"novo_status"` // "livre", "ocupado", "offline"
    De         string `json:"de"`
    Timestamp  int64  `json:"lamport_clock"` // usar isso no Ricart algoritmo
}


// Mensagem JSON para comunicação entre peers
type Mensagem struct {
	Tipo      string      `json:"tipo"`  // STATUS, DATA, HEARTBEAT, REQUEST
	De        string      `json:"de"`    // Zona de origem
	Para      string      `json:"para"`  // Zona de destino (vazio = broadcast)
	Dados     interface{} `json:"dados"` // Payload genérico
	Timestamp time.Time   `json:"timestamp"`
}

// TIPO DE MENSAGENS PARA ATUALIZAR OS DADOS
type SyncRequest struct {
	From string `json:"de"` //zona de origem
	Tipo string `json:"tipo"` // "SINC_REQUEST"
}

type SyncResponse struct {
	Type string `json:"type"` // "SYNC_RESPONSE"
    Drones map[string]Drone `json:"drones"`
}



// MODELS ALGORITMO RICART ======================================================================================
type EstadoRicart string

const (
    EstadoLivre     EstadoRicart = "LIVRE"      // não quer nenhum drone
    EstadoQuerendo  EstadoRicart = "QUERENDO"   // enviou REQUEST, aguardando REPLYs
    EstadoNaSecao   EstadoRicart = "NA_SECAO"   // alocando o drone agora
)

type MensagemRicart struct {
    Tipo      string `json:"tipo"`       // "REQUEST", "REPLY", "RELEASE"
    De        string `json:"de"`
    DroneID   string `json:"drone_id"`  // qual drone está sendo disputado
    Timestamp int64  `json:"timestamp"` // relógio de Lamport
}