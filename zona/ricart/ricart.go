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
	TimestampRequisicao  int64  // timestamp do momento que fez o REQUEST
	PrioridadeRequisicao int    // prioridade da requisição atual
	ReqTimestampFisico   int64  // timestamp fisico para desempate
	DroneAlvo            string // drone que está tentando alocar

	RespostasRecebidas int             // quantos REPLYs já chegaram
	FilaAdiados        []string        // zonas que tiveram REPLY segurado
	EsperandoResposta  map[string]bool // peers que ainda não responderam
	Abortando          bool            // true enquanto AbortarRequisicao está em andamento

	Mu sync.Mutex

	// função injetada para enviar mensagens — evita dependência circular
	EnviarParaZona   func(zonaID string, msg models.Mensagem)
	EnviarParaTodos  func(msg models.Mensagem)
	AoAlocar         func(droneID string) // callback quando conseguir alocar
	AoFalharAlocacao func()               //  chamado quando drone já foi pego
	TotalPeers       func() int           // injetando
	PeersAtivos      func() []string      // injetando também
	TentarAlocar     func()               // callback para processar fila após RELEASE recebido
	ReenfileirarReq  func(req models.Requisicao) // callback para recolocar req na fila após abort

	RequisicaoAtual *models.Requisicao // requisição que originou o REQUEST
}

// IniciarRequisicao — zona quer alocar um drone
func (r *Ricart) IniciarRequisicao(droneID string, req models.Requisicao) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	// Se já estou querendo ou usando um drone, não inicio outra requisição
	if r.Estado == models.EstadoQuerendo || r.Estado == models.EstadoNaSecao {
		log.Printf("[RICART] ⚠ Ignorado: já existe requisição em andamento para %s\n", r.DroneAlvo)
		return
	}

	r.RequisicaoAtual = &req // só salva se vai de fato iniciar

	// Atualiza relógio de Lamport e dados da requisição
	r.RelogioLamport++
	r.TimestampRequisicao = r.RelogioLamport
	r.PrioridadeRequisicao = req.Prioridade
	r.ReqTimestampFisico = req.Timestamp.UnixNano()
	r.Estado = models.EstadoQuerendo
	r.DroneAlvo = droneID
	r.RespostasRecebidas = 0
	r.FilaAdiados = []string{}
	r.Abortando = false

	// Inicializa o mapa de quem está respondendo, esperar resposta de todos os peers ATIVOS no momento do REQUEST
	r.EsperandoResposta = make(map[string]bool)
	for _, peer := range r.PeersAtivos() {
		r.EsperandoResposta[peer] = true
	}

	log.Printf("[RICART] ── Iniciando REQUEST ── drone=%s ts=%d prior=%d aguardando=%d peers: %v\n",
		droneID, r.RelogioLamport, req.Prioridade, len(r.EsperandoResposta), r.PeersAtivos())

	// monta mensagem de REQUEST para todos os peers
	msg := models.Mensagem{
		Tipo: "REQUEST",
		De:   r.ZonaID,
		Dados: models.MensagemRicart{
			Tipo:         "REQUEST",
			De:           r.ZonaID,
			DroneID:      droneID,
			Timestamp:    r.RelogioLamport,
			Prioridade:   req.Prioridade,
			ReqTimestamp: req.Timestamp.UnixNano(),
		},
	}
	r.EnviarParaTodos(msg)

	// Se não tem peers online, já aloca imediatamente. Caso contrário, o watchdog vai garantir que mesmo se algum peer falhar, a requisição não fique travada para sempre.
	if r.RespostasRecebidas >= r.TotalPeers() && !r.Abortando {
		r.Estado = models.EstadoNaSecao
		log.Printf("[RICART] ✔ QUORUM IMEDIATO (zero peers online) → alocando drone %s\n", droneID)
		go r.AoAlocar(droneID)
	} else {
		// Inicia watchdog para garantir que se algum peer falhar, a requisição não fique travada para sempre
		go r.watchdog(droneID, r.TimestampRequisicao, 15*time.Second)
	}
}

// NotificarPeerOffline — chamado quando um peer desconecta.
// Se estávamos esperando REPLY dele, conta como REPLY implícito imediatamente.
func (r *Ricart) NotificarPeerOffline(peerID string) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	if r.Estado != models.EstadoQuerendo {
		return
	}

	if !r.EsperandoResposta[peerID] {
		return // já havia respondido ou não estava na lista
	}

	delete(r.EsperandoResposta, peerID)
	r.RespostasRecebidas++
	log.Printf("[RICART] Peer %s OFFLINE → contado como REPLY implícito (%d/%d)\n",
		peerID, r.RespostasRecebidas, r.TotalPeers())

	if r.RespostasRecebidas >= r.TotalPeers() && !r.Abortando {
		r.Estado = models.EstadoNaSecao
		log.Printf("[RICART] ✔ QUORUM (peer offline) → alocando drone %s\n", r.DroneAlvo)
		go r.AoAlocar(r.DroneAlvo)
	}
}


func (r *Ricart) ReceberRequest(de string, droneID string, timestampDele int64, prioridadeDele int, reqTimestampDele int64) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

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
		// Desempate por prioridade: maior prioridade ganha.
		// Em caso de empate de prioridade, desempata por timestamp (menor ts ganha).
		// Em caso de empate de timestamp, desempata por nome de zona (menor nome ganha).
		euPerco := false
		if prioridadeDele != r.PrioridadeRequisicao {
			euPerco = prioridadeDele > r.PrioridadeRequisicao // ele tem prioridade maior → eu perco
		} else if timestampDele != r.TimestampRequisicao {
			euPerco = timestampDele < r.TimestampRequisicao // mesmo prior, ele enviou antes → eu perco
		} else if reqTimestampDele != r.ReqTimestampFisico {
			euPerco = reqTimestampDele < r.ReqTimestampFisico // desempate pelo tempo fisico da ocorrencia
		} else {
			euPerco = de < r.ZonaID // mesmo prior e ts, nome menor ganha → eu perco
		}
		
		if euPerco {
			// Se eu perdi para uma requisição que chegou agora, e eu já tinha
			// recebido um REPLY ou RELEASE desse peer, eu preciso "devolver"
			// essa permissão, pois ele vai passar na minha frente.
			if !r.EsperandoResposta[de] {
				r.EsperandoResposta[de] = true
				r.RespostasRecebidas--
				log.Printf("[RICART] ⚠ Revogando permissao anterior de %s pois ele tem maior prioridade\n", de)
			}
		}
		
		deveAdiar = !euPerco
	}

	if deveAdiar {
		log.Printf("[RICART] ↷ Adiando REPLY para %s (drone=%s prior=%d) — tenho prioridade (minha=%d)\n",
			de, droneID, prioridadeDele, r.PrioridadeRequisicao)
		r.FilaAdiados = append(r.FilaAdiados, de)
	} else {
		log.Printf("[RICART] ↩ Enviando REPLY para %s (drone=%s)\n", de, droneID)
		r.enviarReply(de, droneID)
	}
}

// ReceberReply — chegou um REPLY de outro peer
func (r *Ricart) ReceberReply(de string, droneID string) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	if r.Estado != models.EstadoQuerendo || r.DroneAlvo != droneID {
		return
	}

	// Só conta se ainda estava esperando resposta deste peer
	// (evita duplicata quando RELEASE já contou como REPLY implícito)
	if !r.EsperandoResposta[de] {
		log.Printf("[RICART] ✉ REPLY duplicado de %s — ignorado\n", de)
		return
	}

	delete(r.EsperandoResposta, de)
	r.RespostasRecebidas++

	log.Printf("[RICART] ✉ REPLY de %s (%d/%d)\n", de, r.RespostasRecebidas, r.TotalPeers())

	if r.RespostasRecebidas >= r.TotalPeers() && !r.Abortando {
		r.Estado = models.EstadoNaSecao
		log.Printf("[RICART] ✔ QUORUM atingido → alocando drone %s\n", droneID)
		go r.AoAlocar(droneID)
	}
}

// ReceberRelease — peer liberou o drone
func (r *Ricart) ReceberRelease(de string, droneID string) {
	log.Printf("[RICART] 🔓 RELEASE de %s (drone=%s)\n", de, droneID)

	r.Mu.Lock()
	// Se estávamos QUERENDO e este peer ainda não tinha respondido, conta como REPLY implícito
	if r.Estado == models.EstadoQuerendo && r.DroneAlvo == droneID {
		if r.EsperandoResposta[de] {
			delete(r.EsperandoResposta, de)
			r.RespostasRecebidas++
			log.Printf("[RICART] 🔓 RELEASE de %s → REPLY implícito (%d/%d)\n", de, r.RespostasRecebidas, r.TotalPeers())
			if r.RespostasRecebidas >= r.TotalPeers() && !r.Abortando {
				r.Estado = models.EstadoNaSecao
				log.Printf("[RICART] ✔ QUORUM (via RELEASE) → alocando drone %s\n", droneID)
				r.Mu.Unlock()
				go r.AoAlocar(droneID)
				return
			}
		}
	}
	r.Mu.Unlock()

	// Se estamos LIVRES, tenta processar a fila local
	if r.TentarAlocar != nil {
		go r.TentarAlocar()
	}
}

// Liberar — chama após terminar de usar o drone
func (r *Ricart) Liberar(droneID string) {
	r.Mu.Lock()
    // Limpa TUDO para a próxima requisição poder entrar
    r.Estado = models.EstadoLivre
    r.DroneAlvo = ""
    r.RequisicaoAtual = nil 
    r.RespostasRecebidas = 0
    adiados := r.FilaAdiados
    r.FilaAdiados = []string{}
    r.Mu.Unlock()

    log.Printf("[RICART] ── Reset completo. Livre para próxima requisição.\n")

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

// AbortarRequisicao — abandona uma requisição em andamento porque descobrimos
// (via DRONE_UPDATE de outro peer) que o drone alvo já foi alocado por outra zona.
// Recoloca a requisição na fila, envia REPLYs adiados e tenta alocar novamente
// (possivelmente com outro drone ou após o atual ser liberado).
func (r *Ricart) AbortarRequisicao(droneID string) {
	r.Mu.Lock()

	// Só aborta se ainda for a requisição ativa para este drone
	if r.Estado != models.EstadoQuerendo || r.DroneAlvo != droneID {
		r.Mu.Unlock()
		return
	}

	log.Printf("[RICART] ✗ Abortando requisição para %s — drone já alocado por outro peer\n", droneID)

	// Seta Abortando DENTRO do lock — qualquer goroutine que adquira o lock
	// agora (ReceberRelease, watchdog, NotificarPeerOffline) vai ver este flag
	// e não vai acionar AoAlocar.
	r.Abortando = true

	// Salva o que precisamos antes de limpar
	adiados := r.FilaAdiados
	reqParaReenfileirar := r.RequisicaoAtual

	r.Estado = models.EstadoLivre
	r.DroneAlvo = ""
	r.RequisicaoAtual = nil
	r.RespostasRecebidas = 0
	r.FilaAdiados = []string{}
	r.EsperandoResposta = make(map[string]bool)
	r.Mu.Unlock()

	// Recoloca a requisição na fila para não perder o pedido
	if reqParaReenfileirar != nil && r.ReenfileirarReq != nil {
		log.Printf("[RICART] ↩ Recolocando req '%s' na fila após abort\n", reqParaReenfileirar.Ocorrencia)
		r.ReenfileirarReq(*reqParaReenfileirar)
	}

	log.Printf("[RICART] ── Abort completo. Enviando REPLYs adiados e voltando para fila.\n")

	for _, zona := range adiados {
		r.enviarReply(zona, droneID)
	}

	// Limpa o flag e tenta a próxima requisição
	r.Mu.Lock()
	r.Abortando = false
	r.Mu.Unlock()

	if r.TentarAlocar != nil {
		go r.TentarAlocar()
	}
}


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

	r.Mu.Lock()
	defer r.Mu.Unlock()

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

	log.Printf("[RICART] ⏱ Timeout! Peers sem resposta: %v → assumindo offline\n", pendentes)

	// Conta os mortos como se tivessem respondido
	r.RespostasRecebidas += len(pendentes)
	for _, peer := range pendentes {
		delete(r.EsperandoResposta, peer)
	}

	// Verifica se agora tem quorum
	if r.RespostasRecebidas >= r.TotalPeers() && !r.Abortando {
		r.Estado = models.EstadoNaSecao
		log.Printf("[RICART] ✔ QUORUM (timeout) → alocando drone %s\n", droneID)
		go r.AoAlocar(droneID)
	}
}