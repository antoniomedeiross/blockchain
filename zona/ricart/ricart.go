package ricart

import (
	//"encoding/json"
	"log"
	"pbl-2/zona/models"
	"sync"
	"time"
)

type Ricart struct {
	ZonaID              string
	Estado              models.EstadoRicart
	RelogioLamport      int64
	TimestampRequisicao int64  // timestamp do momento que fez o REQUEST
	DroneAlvo           string // drone que está tentando alocar

	RespostasRecebidas int             // quantos REPLYs já chegaram
	FilaAdiados        []string        // zonas que tiveram REPLY segurado
	EsperandoResposta  map[string]bool // peers que ainda não responderam

	mu sync.Mutex

	// função injetada para enviar mensagens — evita dependência circular
	EnviarParaZona   func(zonaID string, msg models.Mensagem)
	EnviarParaTodos  func(msg models.Mensagem)
	AoAlocar         func(droneID string) // callback quando conseguir alocar
	AoFalharAlocacao func()               //  chamado quando drone já foi pego
	TotalPeers       func() int           // injetando
	PeersAtivos      func() []string      // injetando também
}

// IniciarRequisicao — zona quer alocar um drone
func (r *Ricart) IniciarRequisicao(droneID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Estado == models.EstadoQuerendo || r.Estado == models.EstadoNaSecao {
		log.Printf("[RICART] Já existe uma requisição em andamento, ignorando\n")
		return
	}

	r.RelogioLamport++
	r.TimestampRequisicao = r.RelogioLamport
	r.Estado = models.EstadoQuerendo
	r.DroneAlvo = droneID
	r.RespostasRecebidas = 0
	r.FilaAdiados = []string{}

	// Marca quem você está esperando
	r.EsperandoResposta = make(map[string]bool)
	for _, peer := range r.PeersAtivos() {
		r.EsperandoResposta[peer] = true
	}

	log.Printf("[RICART] Iniciando requisição para drone %s (timestamp=%d), esperando %d peers\n",
		droneID, r.RelogioLamport, len(r.EsperandoResposta))

	msg := models.Mensagem{
		Tipo: "REQUEST",
		De:   r.ZonaID,
		Dados: models.MensagemRicart{
			Tipo:      "REQUEST",
			De:        r.ZonaID,
			DroneID:   droneID,
			Timestamp: r.RelogioLamport,
		},
	}
	r.EnviarParaTodos(msg)

	// Dispara o watchdog em goroutine separada
	go r.watchdog(droneID, r.TimestampRequisicao, 15*time.Second)
}

// ReceberRequest — chegou um REQUEST de outro peer
func (r *Ricart) ReceberRequest(de string, droneID string, timestampDele int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Atualiza relógio de Lamport
	if timestampDele > r.RelogioLamport {
		r.RelogioLamport = timestampDele
	}
	r.RelogioLamport++

	deveAdiar := false

	if r.Estado == models.EstadoNaSecao && r.DroneAlvo == droneID {
		// Só adia se for o mesmo drone que estou usando
		deveAdiar = true
	} else if r.Estado == models.EstadoQuerendo && r.DroneAlvo == droneID {
		euPerco := timestampDele < r.TimestampRequisicao ||
			(timestampDele == r.TimestampRequisicao && de < r.ZonaID)
		deveAdiar = !euPerco
	}
	// Qualquer outro caso: responde imediatamente

	if deveAdiar {
		log.Printf("[RICART] Adiando REPLY para %s (drone=%s)\n", de, droneID)
		r.FilaAdiados = append(r.FilaAdiados, de)
	} else {
		log.Printf("[RICART] Enviando REPLY para %s (drone=%s)\n", de, droneID)
		r.enviarReply(de, droneID)
	}
}

// ReceberReply — chegou um REPLY de outro peer
func (r *Ricart) ReceberReply(de string, droneID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Estado != models.EstadoQuerendo || r.DroneAlvo != droneID {
		return
	}

	// Marca que este peer respondeu
	delete(r.EsperandoResposta, de)
	r.RespostasRecebidas++

	log.Printf("[RICART] REPLY recebido de %s (%d/%d)\n", de, r.RespostasRecebidas, r.TotalPeers())

	if r.RespostasRecebidas >= r.TotalPeers() {
		r.Estado = models.EstadoNaSecao
		log.Printf("[RICART] Quorum atingido! Alocando drone %s\n", droneID)
		go r.AoAlocar(droneID)
	}
}

// ReceberRelease — peer liberou o drone
func (r *Ricart) ReceberRelease(de string, droneID string) {
	log.Printf("[RICART] RELEASE recebido de %s para drone %s\n", de, droneID)
	// O estado do drone já será atualizado via DRONE_UPDATE
	// Nada a fazer aqui além de logar
}

// Liberar — chama após terminar de usar o drone
func (r *Ricart) Liberar(droneID string) {
	r.mu.Lock()
	adiados := r.FilaAdiados
	r.FilaAdiados = []string{}
	r.Estado = models.EstadoLivre
	r.DroneAlvo = ""
	r.mu.Unlock()

	log.Printf("[RICART] Liberando drone %s, enviando REPLY para %d adiados\n", droneID, len(adiados))

	// Envia RELEASE para todos
	r.EnviarParaTodos(models.Mensagem{
		Tipo: "RELEASE",
		De:   r.ZonaID,
		Dados: models.MensagemRicart{
			Tipo:    "RELEASE",
			De:      r.ZonaID,
			DroneID: droneID,
		},
	})

	// Envia REPLYs que estavam adiados
	for _, zona := range adiados {
		r.enviarReply(zona, droneID)
	}
}

// enviarReply — internal
func (r *Ricart) enviarReply(para string, droneID string) {
	r.EnviarParaZona(para, models.Mensagem{
		Tipo: "REPLY",
		De:   r.ZonaID,
		Dados: models.MensagemRicart{
			Tipo:    "REPLY",
			De:      r.ZonaID,
			DroneID: droneID,
		},
	})
}

// FUNCAO QUE RODA EM BACKGROUND PARA VER SE O REQUEST FOI RESPONDIDO OU SE DEVE ASSUMIR QUE ALGUM PEER FALHOU
func (r *Ricart) watchdog(droneID string, timestampOriginal int64, timeout time.Duration) {
	time.Sleep(timeout)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Verifica se ainda é a mesma requisição ativa
	if r.Estado != models.EstadoQuerendo ||
		r.DroneAlvo != droneID ||
		r.TimestampRequisicao != timestampOriginal {
		return // requisição já foi resolvida, ignora
	}

	// Conta quantos peers ainda não responderam
	pendentes := []string{}
	for peer, esperando := range r.EsperandoResposta {
		if esperando {
			pendentes = append(pendentes, peer)
		}
	}

	if len(pendentes) == 0 {
		return // todos já responderam
	}

	log.Printf("[RICART] Timeout! Peers sem resposta: %v — assumindo mortos\n", pendentes)

	// Conta os mortos como se tivessem respondido
	r.RespostasRecebidas += len(pendentes)
	for _, peer := range pendentes {
		delete(r.EsperandoResposta, peer)
	}

	// Verifica se agora tem quorum
	if r.RespostasRecebidas >= r.TotalPeers() {
		r.Estado = models.EstadoNaSecao
		log.Printf("[RICART] Quorum por timeout! Alocando drone %s\n", droneID)
		go r.AoAlocar(droneID)
	}
}
