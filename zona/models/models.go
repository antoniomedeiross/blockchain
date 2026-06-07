package models

import (
	"net"
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

// STRUCT DA REQUISICAO
type Requisicao struct {
    Tipo       string    `json:"tipo"`
    Sensor     string    `json:"sensor"`
    ZonaID     string    `json:"zona_id"` // quem está pagando pela missão (a própria zona)
    Zona       string    `json:"zona"`
    Ocorrencia string    `json:"ocorrencia"`
    Prioridade int       `json:"prioridade"`
    Timestamp  time.Time `json:"timestamp"`
}

// STRUCT DO DRONE
type DroneStatus string
//tipos de estados possiveis
const (
    StatusLivre   DroneStatus = "livre"
    StatusOcupado DroneStatus = "ocupado"
    StatusOffline DroneStatus = "offline"
)

type Drone struct {
    ID          string      `json:"id"`
    Status      DroneStatus `json:"status"`
    ZonaBase    string      `json:"zona_base"`
    ZonaAtual   string      `json:"zona_atual"`    // pode estar atendendo outra zona
    MissaoAtual *Requisicao `json:"missao_atual,omitempty"` // missão em execução (nil se livre)
}

// //STRUCT do server NAO TO USANDO MAIS
// type Server struct {
//     ID     string
//     Peers  map[string]Peer
//     Drones map[string]Drone  // estado local replicado
//     mu     sync.RWMutex      // protege leitura/escrita concorrente
// }

// //STRUCT De update no drone NAO
// type DroneUpdate struct {
//     Tipo       string `json:"tipo"` // "DRONE_UPDATE"
//     DroneID    string `json:"drone_id"`
//     NovoStatus string `json:"novo_status"` // "livre", "ocupado", "offline"
//     De         string `json:"de"`
//     Timestamp  int64  `json:"lamport_clock"` // usar isso no Ricart algoritmo
// }


// Mensagem JSON para comunicação entre peers
type Mensagem struct {
	Tipo      string      `json:"tipo"`  // STATUS, DATA, HEARTBEAT, REQUEST
	De        string      `json:"de"`    // Zona de origem
	Para      string      `json:"para"`  // Zona de destino (vazio = broadcast)
	Dados     interface{} `json:"dados"` // Payload genérico
	Timestamp time.Time   `json:"timestamp"`
}

// TIPO DE MENSAGENS PARA ATUALIZAR OS DADOS
// type SyncRequest struct {
// 	From string `json:"de"` //zona de origem
// 	Tipo string `json:"tipo"` // "SINC_REQUEST"
// }

// type SyncResponse struct {
// 	Type string `json:"type"` // "SYNC_RESPONSE"
//     Drones map[string]Drone `json:"drones"`
// }



// MODELS ALGORITMO RICART ======================================================================================
type EstadoRicart string

const (
    EstadoLivre     EstadoRicart = "LIVRE"      // não quer nenhum drone
    EstadoQuerendo  EstadoRicart = "QUERENDO"   // enviou REQUEST, aguardando REPLYs
    EstadoNaSecao   EstadoRicart = "NA_SECAO"   // alocando o drone agora
)

// MensagemRicart é a estrutura de mensagem usada para comunicação entre peers no algoritmo de Ricart-Agrawala
type MensagemRicart struct {
    Tipo       string `json:"tipo"`        // "REQUEST", "REPLY", "RELEASE"
    De         string `json:"de"`
    DroneID    string `json:"drone_id"`   // qual drone está sendo disputado
    Timestamp  int64  `json:"timestamp"`  // relógio de Lamport
    Prioridade int    `json:"prioridade"` // prioridade da requisição (maior = mais urgente)
    ReqTimestamp int64 `json:"req_timestamp"` // timestamp físico da requisição para desempate
}


// MISSAO DRONE
type Missao struct {
	RequisicaoID string `json:"requisicao_id"`
	Ocorrencia   string `json:"ocorrencia"`
	Prioridade   int    `json:"prioridade"`
}

// MensagemDrone é a estrutura de mensagem usada para comunicação entre drones e zonas
type MensagemDrone struct {
	Tipo      string      `json:"tipo"`
	De        string      `json:"de"`
	DroneID   string      `json:"drone_id"`
	Dados     interface{} `json:"dados,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// AckMissao é enviado pela zona de failover para a zona base quando
// um drone de failover conclui uma missão.
type AckMissao struct {
	DroneID string `json:"drone_id"`
}