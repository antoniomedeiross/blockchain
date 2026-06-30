package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"pbl-2/blockchain"
	"pbl-2/zona/handler"
	"pbl-2/zona/ledger"
	"pbl-2/zona/models"
	"pbl-2/zona/repo"
	"pbl-2/zona/ricart"
	"strings"
	"time"
)

// --- VARIÁVEIS GLOBAIS -------------------------------------------------
var RicartInstance *ricart.Ricart

// StatusHTTP é o payload JSON retornado pelo endpoint /status
type StatusHTTP struct {
	Zona              string           `json:"zona"`
	Ricart            string           `json:"ricart"`
	Drones            []DroneHTTP      `json:"drones"`
	Fila              []RequisicaoHTTP `json:"fila"`
	Peers             []PeerHTTP       `json:"peers"`
	DronesGerenciados []string         `json:"drones_gerenciados"`
	Ledger            []ledger.Bloco   `json:"ledger"`
	Saldos            map[string]int   `json:"saldos"`
}

type DroneHTTP struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	ZonaBase   string `json:"zona_base"`
	ZonaAtual  string `json:"zona_atual"`
	Missao     string `json:"missao,omitempty"`
	Prioridade int    `json:"prioridade,omitempty"`
}

type RequisicaoHTTP struct {
	Sensor     string `json:"sensor"`
	Ocorrencia string `json:"ocorrencia"`
	Prioridade int    `json:"prioridade"`
}

type PeerHTTP struct {
	Zona string `json:"zona"`
	Vivo bool   `json:"vivo"`
}

// --- FUNÇÕES AUXILIARES -------------------------------------------------

// Funcao para buscar os ips das zonas via variaveis de ambiente
func buscarPears() []string {
	addrs := os.Getenv("PEARS")
	lista := strings.Split(addrs, ",")
	log.Println(lista)
	return lista
}

func getZona() string {
	zona := os.Getenv("ZONA")
	if zona == "" {
		return "DESCONHECIDA"
	}
	return zona
}

// Funcao para conectar aos peers
func conectarAosPeers(peers []string) {
	for _, peer := range peers {
		peerAddr := strings.TrimSpace(peer)
		if peerAddr == "" {
			continue
		}

		// Conecta a cada peer em uma goroutine separada para não bloquear o main
		go func(addr string) {
			for {
				log.Printf("[P2P] Conectando a %s...\n", addr)

				// Timeout de 5s para não travar se o peer estiver offline
				conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
				if err != nil {
					log.Printf("[P2P] %s offline — tentando em 3s\n", addr)
					time.Sleep(3 * time.Second)
					continue
				}

				log.Printf("[P2P] ✔ Conectado a %s\n", addr)

				// 1. Enviar identificação
				minhaZona := getZona()
				meuAddr := os.Getenv("MY_ADDR")

				conn.Write([]byte("IAM:PEER:" + minhaZona + ":" + meuAddr + "\n"))

				// 2. Aguardar confirmação
				leitor := bufio.NewReader(conn)
				resp, _ := leitor.ReadString('\n')
				if strings.TrimSpace(resp) != "OK" {
					log.Printf("[P2P] ✗ Falha ao identificar em %s\n", addr)
					conn.Close()
					time.Sleep(2 * time.Second)
					continue
				}

				log.Printf("[P2P] ✔ Identificado em %s\n", addr)

				// 3. Pedir estado dos drones e SINCRONIZAR LEDGER
				minhaZona = getZona()
				meuTamanho := 0
				if ledger.Instancia != nil {
					meuTamanho = ledger.Instancia.Tamanho()
				}

				syncReq := models.Mensagem{
					Tipo:      "SYNC_REQUEST",
					De:        minhaZona,
					Dados:     map[string]interface{}{"chain_size": meuTamanho, "pubkey": ledger.ChavePublicaDaZona()},
					Timestamp: time.Now(),
				}
				data, _ := json.Marshal(syncReq)
				conn.Write(append(data, '\n'))

				// 4. Loop de leitura de mensagens
				for {
					msg, err := leitor.ReadString('\n')
					if err != nil {
						log.Printf("[P2P] ✗ Conexão com %s perdida\n", addr)
						conn.Close()

						// Notifica o Ricart: busca o ZonaID deste peer pelo endereço
						repo.Mutex.RLock()
						for zonaID, peer := range repo.Peers {
							if peer.Address == addr {
								repo.Mutex.RUnlock()
								repo.RicartInstance.NotificarPeerOffline(zonaID)
								goto reconectar
							}
						}
						repo.Mutex.RUnlock()
					reconectar:
						break
					}

					// processar mensagem recebida ------------------------------------------

					msg = strings.TrimSpace(msg)
					if msg == "" {
						continue
					}

					var mensagem models.Mensagem

					if err := json.Unmarshal([]byte(msg), &mensagem); err != nil {
						log.Printf("Erro ao deserializar mensagem de %s: %v\n", addr, err)
						continue
					}

					// Processa a mensagem de acordo com o tipo

					switch mensagem.Tipo {

					case "BLOCO":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var bloco ledger.Bloco
						if err := json.Unmarshal(dadosJSON, &bloco); err != nil {
							log.Printf("Erro ao parsear BLOCO: %v\n", err)
							continue
						}
						ledger.AceitarBlocoExterno(bloco)

					// BLOCO_PROPOSTA: peer minerou um bloco e pede nosso voto antes de confirmar
					case "BLOCO_PROPOSTA":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var bloco ledger.Bloco
						if err := json.Unmarshal(dadosJSON, &bloco); err != nil {
							log.Printf("Erro ao parsear BLOCO_PROPOSTA: %v\n", err)
							continue
						}
						aceito := true
						motivo := ""
						if !bloco.Validar() {
							aceito = false
							motivo = "hash inválido ou PoW insuficiente"
						} else if ledger.Instancia != nil {
							snap := ledger.Instancia.Snapshot()
							if len(snap) > 0 {
								topo := snap[len(snap)-1]
								if bloco.Index != topo.Index+1 {
									aceito = false
									motivo = "índice fora de ordem"
								} else if bloco.HashAnterior != topo.Hash {
									aceito = false
									motivo = "hash anterior não confere"
								}
							}
							if aceito && bloco.Tx.Tipo == ledger.TxPagamento {
								if ledger.ConsultarSaldo(bloco.Tx.ZonaID) < bloco.Tx.Creditos {
									aceito = false
									motivo = "saldo insuficiente"
								}
							}
						}
						if aceito {
							log.Printf("[CONSENSUS] ✔ Votando SIM no bloco #%d de %s\n", bloco.Index, mensagem.De)
							ledger.AceitarBlocoExterno(bloco)
						} else {
							log.Printf("[CONSENSUS] ✗ Votando NÃO no bloco #%d de %s: %s\n", bloco.Index, mensagem.De, motivo)
						}
						voto := models.VotoConsensus{HashBloco: bloco.Hash, Aceito: aceito, Motivo: motivo}
						respVoto := models.Mensagem{
							Tipo:      "VOTO_CONSENSO",
							De:        getZona(),
							Dados:     voto,
							Timestamp: time.Now(),
						}
						if data, err := json.Marshal(respVoto); err == nil {
							conn.Write(append(data, '\n'))
						}

					// VOTO_CONSENSO: peer respondeu nossa proposta — encaminha para o canal de votos
					case "VOTO_CONSENSO":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var voto models.VotoConsensus
						if err := json.Unmarshal(dadosJSON, &voto); err != nil {
							log.Printf("Erro ao parsear VOTO_CONSENSO: %v\n", err)
							continue
						}
						if ch, ok := blockchain.PendingConsensus.Load(voto.HashBloco); ok {
							ch.(chan bool) <- voto.Aceito
						}

					// SYNC_RESPONSE é a resposta ao SYNC_REQUEST enviado logo após a conexão
					case "SYNC_RESPONSE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var drones map[string]models.Drone
						if err := json.Unmarshal(dadosJSON, &drones); err != nil {
							log.Printf("Erro ao parsear SYNC_RESPONSE: %v\n", err)
							continue
						}

						// Atualiza o estado local com os drones do peer
						for _, d := range drones {
							repo.AtualizarDroneRemoto(d)
						}
						log.Printf("[SYNC] ✔ Sincronizado com %s: %d drone(s) no estado\n", addr, len(drones))

						// Tenta processar fila local após sincronizar drones
						go func() {
							time.Sleep(1 * time.Second)
							repo.TentarAlocarDaFila()
						}()

					// DRONE_UPDATE é enviado por um peer quando um drone muda de estado (alocado, liberado, offline)
					case "DRONE_UPDATE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var drone models.Drone
						if err := json.Unmarshal(dadosJSON, &drone); err != nil {
							log.Printf("Erro ao parsear DRONE_UPDATE: %v\n", err)
							continue
						}
						repo.AtualizarDroneRemoto(drone)
						log.Printf("[ESTADO] Drone %s → %s (via %s)\n", drone.ID, drone.Status, addr)
						// Drone ficou livre (ex: reconectou em outra zona após failover).
						// Tenta processar fila local que pode estar aguardando por ele.
						if drone.Status == models.StatusLivre {
							go func() {
								time.Sleep(200 * time.Millisecond)
								repo.TentarAlocarDaFila()
							}()
						}

					// DRONES_RESPONSE é a resposta ao pedido de lista de drones (DEBUUUUG)
					case "DRONES_RESPONSE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var drones map[string]models.Drone
						if err := json.Unmarshal(dadosJSON, &drones); err != nil {
							log.Printf("Erro ao parsear DRONES_RESPONSE: %v\n", err)
							continue
						}
						log.Printf("\n┌─── DRONES DE %s ───\n", mensagem.De)
						for _, d := range drones {
							log.Printf("  ID: %s | Status: %s | Base: %s | Atual: %s\n",
								d.ID, d.Status, d.ZonaBase, d.ZonaAtual)
						}
						log.Printf("└────────────────────────────\n")

					// MENSAGENS DO SENSOR E RICART (REQUISICAO_DRONE, REQUEST, REPLY, RELEASE) são processadas no handler para não misturar lógica de rede com lógica de alocação
					case "REQUEST", "REPLY", "RELEASE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var ricartMsg models.MensagemRicart
						if err := json.Unmarshal(dadosJSON, &ricartMsg); err != nil {
							log.Printf("Erro ao parsear mensagem Ricart: %v\n", err)
							continue
						}
						switch mensagem.Tipo {

						// se for REQUEST, passa para o Ricart processar a requisição de outro peer
						case "REQUEST":
							repo.RicartInstance.ReceberRequest(ricartMsg.De, ricartMsg.DroneID, ricartMsg.Timestamp, ricartMsg.Prioridade, ricartMsg.ReqTimestamp)

						// se for REPLY, passa para o Ricart processar a resposta de outro peer
						case "REPLY":
							repo.RicartInstance.ReceberReply(ricartMsg.De, ricartMsg.DroneID)

						// se for RELEASE, passa para o Ricart processar a liberação de outro peer
						case "RELEASE":
							repo.RicartInstance.ReceberRelease(ricartMsg.De, ricartMsg.DroneID)

						// se for REQUISICAO_DRONE, é uma nova requisição vinda de outro peer — tenta alocar um drone para ela
						case "REQUISICAO_DRONE":
							dadosJSON, _ := json.Marshal(mensagem.Dados)
							var req models.Requisicao
							if err := json.Unmarshal(dadosJSON, &req); err != nil {
								log.Printf("Erro ao parsear REQUISICAO_DRONE: %v\n", err)
								continue
							}
							log.Printf("[SENSOR] ► Requisição de %s (zona %s) — ocorrência: %s, prioridade: %d\n",
								req.Sensor, req.ZonaID, req.Ocorrencia, req.Prioridade)

							// Dispara alocação de drone
							go func() {
								drone, ok := repo.SelecionarDroneLivre()
								if !ok {
									log.Printf("[FILA] ↓ Enfileirando req de %s (prioridade=%d, ocorrência=%s) — sem drone livre\n", req.Sensor, req.Prioridade, req.Ocorrencia)
									// fila vem no próximo passo
									return
								}
								// Se tem drone livre, tenta alocar imediatamente sem passar pela fila
								repo.RicartInstance.IniciarRequisicao(drone.ID, req)
							}()
						}

					// DESPACHAR_DRONE é uma mensagem interna entre peers para enviar a missão para o drone físico na zona correta
					case "DESPACHAR_DRONE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var missao models.MensagemDrone
						if err := json.Unmarshal(dadosJSON, &missao); err != nil {
							log.Printf("Erro ao parsear DESPACHAR_DRONE: %v\n", err)
							continue
						}

						log.Printf("[MISSÃO] ◄ Recebi DESPACHAR de peer — enviando missão para %s\n", missao.DroneID)

						// Pega a struct, transforma em byte de novo e joga no socket do drone físico
						data, _ := json.Marshal(missao)
						if !repo.EnviarParaDrone(missao.DroneID, data) {
							log.Printf("[MISSÃO] ✗ ERRO: Drone %s não está conectado aqui\n", missao.DroneID)
						}

					}

				}

				// Se a conexão cair, tenta reconectar depois de um tempo
				log.Printf("[P2P] ↻ Reconectando com %s...\n", addr)
				time.Sleep(5 * time.Second)
			}
		}(peerAddr)
	}
}

// Funcao para enviar mensagem JSON para todos peers (broadcast pelos recebidos)
func enviarParaTodos(mensagem models.Mensagem) {
	repo.Mutex.RLock()
	peers := make(map[string]models.Peer)
	for k, v := range repo.Peers {
		peers[k] = v
	}
	repo.Mutex.RUnlock()

	data, _ := json.Marshal(mensagem)
	for _, peer := range peers {
		if peer.Alive && peer.Conn != nil {
			peer.Conn.Write(append(data, '\n'))
		}
	}
}

// funcao q envia pra uma zona apenas
func enviarParaZona(zonaID string, mensagem models.Mensagem) {
	repo.Mutex.RLock()
	peer, exists := repo.Peers[zonaID]
	repo.Mutex.RUnlock()

	if !exists || !peer.Alive || peer.Conn == nil {
		log.Printf("[P2P] ✗ Peer %s não encontrado ou offline\n", zonaID)
		return
	}

	data, _ := json.Marshal(mensagem)
	peer.Conn.Write(append(data, '\n'))
}

// FUNCAO DE BROADCAST ENTRE OS PEEARS
func BroadcastDroneUpdate(drone models.Drone) {
	repo.Mutex.RLock()
	// copia do mapa de peers para enviar update
	peers := make(map[string]models.Peer)
	for k, v := range repo.Peers {
		peers[k] = v
	}
	repo.Mutex.RUnlock()

	// cria mensagem de update do drone para enviar a todos os peers
	mensagem := models.Mensagem{
		Tipo:      "DRONE_UPDATE",
		De:        getZona(),
		Dados:     drone,
		Timestamp: time.Now(),
	}

	// transforma a mensagem em JSON e envia para todos os peers vivos
	data, _ := json.Marshal(mensagem)
	for _, peer := range peers {
		if peer.Alive && peer.Conn != nil {
			peer.Conn.Write(append(data, '\n'))
		}
	}
}

// Porta HTTP para a interface consumir os dados dos peers
func iniciarHTTP() {
	mux := http.NewServeMux()

	// /status — retorna estado completo desta zona em JSON
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		// Drones
		dronesRaw := repo.BuscarDrones()
		drones := make([]DroneHTTP, 0, len(dronesRaw))
		for _, d := range dronesRaw {
			entry := DroneHTTP{
				ID:        d.ID,
				Status:    string(d.Status),
				ZonaBase:  d.ZonaBase,
				ZonaAtual: d.ZonaAtual,
			}
			if d.MissaoAtual != nil {
				entry.Missao = d.MissaoAtual.Ocorrencia
				entry.Prioridade = d.MissaoAtual.Prioridade
			}
			drones = append(drones, entry)
		}

		// Fila
		repo.FilaMutex.Lock()
		fila := make([]RequisicaoHTTP, 0, repo.RequisicoesPendentes.Len())
		for _, req := range *repo.RequisicoesPendentes {
			fila = append(fila, RequisicaoHTTP{
				Sensor:     req.Sensor,
				Ocorrencia: req.Ocorrencia,
				Prioridade: req.Prioridade,
			})
		}
		repo.FilaMutex.Unlock()

		// Peers
		repo.Mutex.RLock()
		peers := make([]PeerHTTP, 0, len(repo.Peers))
		for zona, p := range repo.Peers {
			peers = append(peers, PeerHTTP{Zona: zona, Vivo: p.Alive})
		}
		repo.Mutex.RUnlock()

		// Estado Ricart
		ricartEstado := "LIVRE"
		if repo.RicartInstance != nil {
			repo.RicartInstance.Mu.Lock()
			ricartEstado = string(repo.RicartInstance.Estado)
			repo.RicartInstance.Mu.Unlock()
		}

		// Buscar informações do Ledger
		blocos := []ledger.Bloco{}
		saldos := make(map[string]int)
		if ledger.Instancia != nil {
			blocos = ledger.Instancia.Snapshot()
			// Vamos calcular o saldo para todas as zonas mapeadas
			for _, b := range blocos {
				if b.Tx.ZonaID != "" {
					saldos[b.Tx.ZonaID] = ledger.ConsultarSaldo(b.Tx.ZonaID)
				}
			}
		}

		// Monta struct de status completo para enviar como JSON no endpoint
		status := StatusHTTP{
			Zona:              getZona(),
			Ricart:            ricartEstado,
			Drones:            drones,
			Fila:              fila,
			Peers:             peers,
			DronesGerenciados: repo.ListarGerenciados(),
			Ledger:            blocos,
			Saldos:            saldos,
		}

		json.NewEncoder(w).Encode(status)
	})

	// /ledger — retorna o arquivo bruto para auditoria
	mux.HandleFunc("/ledger", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		
		// Serve o arquivo ledger.json diretamente
		http.ServeFile(w, r, "ledger.json")
	})

	// /validate — verifica integridade da chain local e retorna JSON com resultado
	mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		type ValidateResp struct {
			Zona   string `json:"zona"`
			Valida bool   `json:"valida"`
			Blocos int    `json:"total_blocos"`
			Motivo string `json:"motivo,omitempty"`
		}

		resp := ValidateResp{Zona: getZona()}
		if ledger.Instancia == nil {
			resp.Valida = false
			resp.Motivo = "ledger não inicializado"
		} else {
			resp.Valida = ledger.Instancia.ValidarChain()
			resp.Blocos = ledger.Instancia.Tamanho()
			if !resp.Valida {
				resp.Motivo = "encadeamento de hashes quebrado — chain adulterada"
			}
		}

		if !resp.Valida {
			w.WriteHeader(http.StatusConflict)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// /seguranca — expõe informações de identidade criptográfica desta zona
	mux.HandleFunc("/seguranca", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ledger.InfoSeguranca())
	})

	// Goroutine de auditoria periódica — valida a chain a cada 30s e loga resultado
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if ledger.Instancia == nil {
				continue
			}
			if ledger.Instancia.ValidarChain() {
				log.Printf("[AUDIT] ✔ Chain íntegra — %d bloco(s) verificados\n",
					ledger.Instancia.Tamanho())
			} else {
				log.Printf("[AUDIT] ✗ ADULTERAÇÃO DETECTADA — chain local inválida!\n")
			}
		}
	}()

	porta := os.Getenv("HTTP_PORT")
	if porta == "" {
		porta = "8080"
	}
	log.Printf("[HTTP] Dashboard disponível em :%s/status\n", porta)
	http.ListenAndServe(":"+porta, mux)
}

// --- MAIN -----------------------------------------------------------------------------
func main() {
	// Iniciar Ledger
	ledger.IniciarLedger(getZona())
	ledger.PropagaBloco = func(bloco ledger.Bloco) {
		enviarParaTodos(models.Mensagem{
			Tipo:      "BLOCO",
			De:        getZona(),
			Dados:     bloco,
			Timestamp: time.Now(),
		})
	}

	// Injeta o mecanismo de consenso 2/3: ProposeAndMine do blockchain package,
	// adaptado para operar sobre a chain do ledger via canais de voto (PendingConsensus).
	ledger.ProporBlocoFn = func(bloco ledger.Bloco) (ledger.Bloco, error) {
		// Conta peers ativos para calcular quorum
		repo.Mutex.RLock()
		totalPeers := 0
		for _, p := range repo.Peers {
			if p.Alive {
				totalPeers++
			}
		}
		repo.Mutex.RUnlock()

		total := totalPeers + 1
		quorum := (total * 2) / 3
		if quorum < 1 {
			quorum = 1
		}

		// Canal de votos com buffer para não bloquear quem vota após timeout
		respostas := make(chan bool, total)
		blockchain.PendingConsensus.Store(bloco.Hash, respostas)
		defer blockchain.PendingConsensus.Delete(bloco.Hash)

		log.Printf("[CONSENSUS] 🗳 Propondo bloco #%d hash=%s quorum=%d/%d\n",
			bloco.Index, bloco.Hash[:12], quorum, total)

		// Propaga para peers votarem
		enviarParaTodos(models.Mensagem{
			Tipo:      "BLOCO_PROPOSTA",
			De:        getZona(),
			Dados:     bloco,
			Timestamp: time.Now(),
		})

		// Conta voto próprio (auto-accept)
		aceitos := 1
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()

		for i := 0; i < totalPeers; i++ {
			select {
			case ok := <-respostas:
				if ok {
					aceitos++
				}
				if aceitos >= quorum {
					goto confirma
				}
			case <-timer.C:
				log.Printf("[CONSENSUS] ⏱ Timeout — %d peer(s) não responderam, assumindo aceite implícito\n",
					totalPeers-i)
				aceitos = total
				goto confirma
			}
		}

	confirma:
		if aceitos < quorum {
			log.Printf("[CONSENSUS] ✗ Bloco #%d rejeitado — votos=%d quorum=%d\n",
				bloco.Index, aceitos, quorum)
			return ledger.Bloco{}, fmt.Errorf("consenso não atingido (%d/%d votos)", aceitos, quorum)
		}

		log.Printf("[CONSENSUS] ✔ Bloco #%d confirmado — %d/%d votos\n", bloco.Index, aceitos, total)
		return bloco, nil
	}

	// Buscar lista de peers
	peers := buscarPears()

	// Conectar aos peers em uma goroutine separada
	go conectarAosPeers(peers)

	// RICART ALGORITMO
	repo.RicartInstance = &ricart.Ricart{
		ZonaID:          getZona(),
		Estado:          models.EstadoLivre,
		EnviarParaTodos: enviarParaTodos,
		EnviarParaZona:  enviarParaZona,
		TotalPeers: func() int {
			repo.Mutex.RLock()
			defer repo.Mutex.RUnlock()
			count := 0
			for _, peer := range repo.Peers {
				if peer.Alive {
					count++
				}
			}
			return count
		},

		AoAlocar: func(droneID string) {
			repo.DroneMutex.Lock()
			drone, existe := repo.Drones[droneID]
			if !existe || drone.Status != models.StatusLivre {
				repo.DroneMutex.Unlock()
				// Drone já foi alocado por outro peer — recoloca a req na fila
				// antes de liberar, senão ela é perdida quando Liberar() limpa RequisicaoAtual
				repo.RicartInstance.Mu.Lock()
				reqPerdida := repo.RicartInstance.RequisicaoAtual
				repo.RicartInstance.Mu.Unlock()

				if reqPerdida != nil {
					log.Printf("[ALOCAÇÃO] ↩ Drone %s já ocupado — recolocando req '%s' na fila\n", droneID, reqPerdida.Ocorrencia)
					repo.Enfileirar(*reqPerdida)
				}

				// Recoloca o drone no estado livre para atualizar o estado distribuído e avisar os outros peers
				repo.RicartInstance.Liberar(droneID)
				// Tenta alocar outro drone da fila, se tiver
				go repo.TentarAlocarDaFila()
				return
			}

			// Drone está livre aqui — aloca localmente e envia missão para o drone físico
			reqAtual := repo.RicartInstance.RequisicaoAtual
			repo.DroneMutex.Unlock()

			// PAGAMENTO NA ALOCAÇÃO: Desconta o saldo apenas quando o drone for efetivamente despachado
			if reqAtual != nil && !reqAtual.Pago {
				err := ledger.RegistrarPagamento(reqAtual.ZonaID, droneID, reqAtual.Ocorrencia, reqAtual.Zona)
				if err != nil {
					log.Printf("[LEDGER] ✗ Falha no pagamento na alocação para %s: %v\n", reqAtual.ZonaID, err)
					// Recoloca a requisição na fila para não perder o pedido (ela fica salva)
					repo.Enfileirar(*reqAtual)
					repo.RicartInstance.Liberar(droneID)
					go repo.TentarAlocarDaFila()
					return
				}
				// Marca como paga para não cobrar de novo se o drone cair
				reqAtual.Pago = true
			} else if reqAtual != nil && reqAtual.Pago {
				log.Printf("[LEDGER] ℹ Missão '%s' já paga anteriormente (reuso de despacho)\n", reqAtual.Ocorrencia)
			}

			missao := models.MensagemDrone{
				Tipo:    "MISSAO",
				De:      getZona(),
				DroneID: droneID,
				Dados: models.Missao{
					RequisicaoID: reqAtual.Sensor,
					Ocorrencia:   reqAtual.Ocorrencia,
					Prioridade:   reqAtual.Prioridade,
				},
				Timestamp: time.Now(),
			}
			data, _ := json.Marshal(missao)

			// Tenta enviar missão para o drone físico conectado aqui
			if repo.EnviarParaDrone(droneID, data) {
				// Drone está conectado fisicamente aqui — atualiza ZonaAtual para cá
				repo.DroneMutex.Lock()
				drone.Status = models.StatusOcupado
				drone.ZonaAtual = getZona()
				drone.MissaoAtual = reqAtual
				repo.Drones[droneID] = drone
				repo.DroneMutex.Unlock()

				// Envia update do drone para os peers para atualizar o estado distribuído
				BroadcastDroneUpdate(drone)
				repo.RegistrarGerenciamento(droneID)
				log.Printf("\n[ALOCAÇÃO] ══► Drone %s alocado localmente em %s\n", droneID, getZona())
				log.Printf("[MISSÃO] ► Missão enviada diretamente para %s\n", droneID)
				// Liberar é chamado quando MISSAO_CONCLUIDA chegar via processarDrone

				// watchdog: se o drone não concluir em 60s, libera
				go func() {
					time.Sleep(60 * time.Second)
					repo.DroneMutex.RLock()
					d := repo.Drones[droneID]
					repo.DroneMutex.RUnlock()

					// Se o drone ainda estiver ocupado e com a mesma missão, considera que ele falhou ou perdeu conexão — libera e tenta alocar outro da fila
					if d.Status == models.StatusOcupado && d.MissaoAtual != nil {
						log.Printf("[WATCHDOG] ⚠ Drone %s não concluiu missão em 60s — liberando\n", droneID)
						repo.DroneMutex.Lock()
						missaoPerdida := d.MissaoAtual
						d.MissaoAtual = nil
						d.Status = models.StatusLivre
						repo.Drones[droneID] = d
						repo.DroneMutex.Unlock()

						repo.Enfileirar(*missaoPerdida)
						repo.RicartInstance.Liberar(droneID)

						// Tenta alocar outro drone da fila, se tiver
						go repo.TentarAlocarDaFila()
					}
				}()

			} else {
				// Drone não está aqui — descobre em qual zona ele está via ZonaAtual do estado distribuído
				// NÃO sobrescreve ZonaAtual para getZona() pois não temos o drone físico
				zonaDestino := drone.ZonaAtual
				if zonaDestino == "" {
					zonaDestino = drone.ZonaBase
				}

				minhaZona := getZona()

				// Se ZonaAtual aponta para nós mas não temos o socket, estado desatualizado

				// Tenta encontrar em qual peer o drone realmente está
				if zonaDestino == minhaZona {
					log.Printf("[MISSÃO] ⚠ Drone %s: ZonaAtual=%s mas não conectado aqui — buscando peer vivo\n", droneID, minhaZona)
					repo.Mutex.RLock()
					zonaDestino = ""
					for id, peer := range repo.Peers {
						if peer.Alive {
							zonaDestino = id
							break
						}
					}
					repo.Mutex.RUnlock()

					// Se não encontrar nenhum peer vivo, libera a requisição para não travar a fila
					if zonaDestino == "" {
						log.Printf("[MISSÃO] ✗ Nenhum peer vivo para drone %s — liberando Ricart\n", droneID)
						repo.RicartInstance.Liberar(droneID)
						return
					}
				}

				// Atualiza status para ocupado mas mantém ZonaAtual original do drone
				repo.DroneMutex.Lock()
				drone.Status = models.StatusOcupado
				drone.MissaoAtual = reqAtual
				// ZonaAtual não é alterado — o drone está onde o estado distribuído diz
				repo.Drones[droneID] = drone
				repo.DroneMutex.Unlock()

				// update em broadcast 
				BroadcastDroneUpdate(drone)
				repo.RegistrarGerenciamento(droneID)
				log.Printf("\n[ALOCAÇÃO] ══► Drone %s alocado remotamente → enviando para %s\n", droneID, zonaDestino)

				// Envia missão para a zona correta do drone — lá o Ricart é liberado quando a missão for despachada para o drone físico, não aqui
				enviarParaZona(zonaDestino, models.Mensagem{
					Tipo:  "DESPACHAR_DRONE",
					De:    getZona(),
					Para:  zonaDestino,
					Dados: missao,
				})
				// Para drones remotos, o Ricart é liberado imediatamente após despachar.
				// O gerenciamento é removido quando o RELEASE confirmar a conclusão.
				repo.RicartInstance.Liberar(droneID)
			}
		},

		AoFalharAlocacao: func() {
			// Não faz mais nada
			// so mostra um aviso 
			log.Printf("[ALOCAÇÃO] ⚠ AoFalharAlocacao chamado — verifique a lógica de seleção\n")
		},

		// PeersAtivos: func() []string {
		PeersAtivos: func() []string {
			repo.Mutex.RLock()
			defer repo.Mutex.RUnlock()
			lista := []string{}
			for id, peer := range repo.Peers {
				if peer.Alive {
					lista = append(lista, id)
				}
			}
			return lista
		},

		// TentarAlocar é chamado pelo Ricart quando um drone fica livre e tem uma requisição aguardando na fila — tenta alocar o drone para a requisição mais prioritária da fila
		TentarAlocar: func() {
			time.Sleep(200 * time.Millisecond) // pequeno delay para o DRONE_UPDATE chegar antes
			repo.TentarAlocarDaFila()
		},

		// ReenfileirarReq é chamado pelo Ricart quando uma requisição falha na alocação (ex: drone alocado por outro peer) e precisa ser reenfileirada para tentar alocar outro drone
		ReenfileirarReq: func(req models.Requisicao) {
			repo.Enfileirar(req)
		},
	}

	// Configura função de broadcast do repositório para enviar atualizações de drone para os peers
	repo.BroadcastFn = BroadcastDroneUpdate

	// // Enviar heartbeat periodicamente /////////////////////////////////////////////////////////////////////////
	// go func() {
	// 	minhaZona := getZona()
	// 	ticker := time.NewTicker(15 * time.Second)
	// 	defer ticker.Stop()

	// 	for range ticker.C {
	// 		mensagem := models.Mensagem{
	// 			Tipo:      "HEARTBEAT",
	// 			De:        minhaZona,
	// 			Para:      "", // broadcast
	// 			Dados:     nil,
	// 			Timestamp: time.Now(),
	// 		}
	// 		enviarParaTodos(mensagem)
	// 	}
	// }()

	// ======== DEBUG ==================================================================
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Mostra drones locais
			drones := repo.BuscarDrones()
			log.Printf("\n┌─── DRONES LOCAIS [%s] ───\n", getZona())
			for _, d := range drones {
				log.Printf("  ID: %s | Status: %s | Base: %s | Atual: %s\n",
					d.ID, d.Status, d.ZonaBase, d.ZonaAtual)
			}
			log.Printf("└──────────────────────────────────\n")

			// Pede para cada peer
			enviarParaTodos(models.Mensagem{
				Tipo:      "GET_DRONES",
				De:        getZona(),
				Timestamp: time.Now(),
			})
		}
	}()

	// Servidor HTTP para interface
	go iniciarHTTP()

	// Abrir servidor para escutar conexoes
	listner, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("TCP rodando na porta 9090")

	// Loop principal para aceitar conexões de drones físicos e peers
	for {
		conn, err := listner.Accept()
		if err != nil {
			continue
		}

		// Cada conexão é processada em uma goroutine separada para não bloquear o loop de aceitação
		go handler.ProcessarConexoes(conn)
	}

}
