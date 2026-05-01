package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"pbl-2/zona/handler"
	"pbl-2/zona/models"
	"pbl-2/zona/repo"
	"pbl-2/zona/ricart"
	"strings"
	"time"
)

var RicartInstance *ricart.Ricart

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
		go func(addr string) {
			for {
				log.Printf("Tentando conectar ao peer: %s\n", addr)
				conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
				if err != nil {
					log.Printf("Peer %s offline, tentando novamente em 3s...\n", addr)
					time.Sleep(3 * time.Second)
					continue
				}
				log.Printf("Sucesso: Conectado ao peer %s\n", addr)

				// 1. Enviar identificação
				minhaZona := getZona()
				meuAddr := os.Getenv("MY_ADDR") 
				conn.Write([]byte("IAM:PEER:" + minhaZona + ":" + meuAddr + "\n"))

				// 2. Aguardar confirmação
				leitor := bufio.NewReader(conn)
				resp, _ := leitor.ReadString('\n')
				if strings.TrimSpace(resp) != "OK" {
					log.Printf("Falha ao se identificar para %s\n", addr)
					conn.Close()
					time.Sleep(2 * time.Second)
					continue
				}

				log.Printf("Identificado com sucesso para %s\n", addr)

				// 3. Pedir estado dos drones
				syncReq := models.Mensagem{
					Tipo:      "SYNC_REQUEST",
					De:        minhaZona,
					Timestamp: time.Now(),
				}
				data, _ := json.Marshal(syncReq)
				conn.Write(append(data, '\n'))

				// 4. Loop de leitura de mensagens
				for {
					msg, err := leitor.ReadString('\n')
					if err != nil {
						log.Printf("Conexão com %s perdida\n", addr)
						conn.Close()
						break
					}
					msg = strings.TrimSpace(msg)
					if msg == "" {
						continue
					}

					var mensagem models.Mensagem
					if err := json.Unmarshal([]byte(msg), &mensagem); err != nil {
						log.Printf("Erro ao deserializar mensagem de %s: %v\n", addr, err)
						continue
					}

					log.Printf("[OUTGOING] Mensagem de %s: Tipo=%s\n", addr, mensagem.Tipo)

					switch mensagem.Tipo {
					case "SYNC_RESPONSE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var drones map[string]models.Drone
						if err := json.Unmarshal(dadosJSON, &drones); err != nil {
							log.Printf("Erro ao parsear SYNC_RESPONSE: %v\n", err)
							continue
						}
						for _, d := range drones {
							repo.AtualizarDrone(d)
						}
						log.Printf("Sincronizado com %s: %d drones recebidos\n", addr, len(drones))

					case "DRONE_UPDATE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var drone models.Drone
						if err := json.Unmarshal(dadosJSON, &drone); err != nil {
							log.Printf("Erro ao parsear DRONE_UPDATE: %v\n", err)
							continue
						}
						repo.AtualizarDrone(drone)
						log.Printf("Drone %s atualizado para: %s\n", drone.ID, drone.Status)

					case "DRONES_RESPONSE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var drones map[string]models.Drone
						if err := json.Unmarshal(dadosJSON, &drones); err != nil {
							log.Printf("Erro ao parsear DRONES_RESPONSE: %v\n", err)
							continue
						}
						log.Printf("\n========= DRONES DE %s =========\n", mensagem.De)
						for _, d := range drones {
							log.Printf("  ID: %s | Status: %s | Base: %s | Atual: %s\n",
								d.ID, d.Status, d.ZonaBase, d.ZonaAtual)
						}
						log.Printf("=================================\n")
					case "REQUEST", "REPLY", "RELEASE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var ricartMsg models.MensagemRicart
						if err := json.Unmarshal(dadosJSON, &ricartMsg); err != nil {
							log.Printf("Erro ao parsear mensagem Ricart: %v\n", err)
							continue
						}
						switch mensagem.Tipo {
						case "REQUEST":
							repo.RicartInstance.ReceberRequest(ricartMsg.De, ricartMsg.DroneID, ricartMsg.Timestamp)
						case "REPLY":
							repo.RicartInstance.ReceberReply(ricartMsg.De, ricartMsg.DroneID)
						case "RELEASE":
							repo.RicartInstance.ReceberRelease(ricartMsg.De, ricartMsg.DroneID)

						case "REQUISICAO_DRONE":
							dadosJSON, _ := json.Marshal(mensagem.Dados)
							var req models.Requisicao
							if err := json.Unmarshal(dadosJSON, &req); err != nil {
								log.Printf("Erro ao parsear REQUISICAO_DRONE: %v\n", err)
								continue
							}
							log.Printf("[ZONA] Requisição recebida do sensor %s — ocorrência: %s, prioridade: %d\n",
								req.Sensor, req.Ocorrencia, req.Prioridade)

							// Dispara alocação de drone
							go func() {
								drone, ok := repo.SelecionarDroneLivre()
								if !ok {
									log.Printf("[ZONA] Nenhum drone livre para atender sensor %s — enfileirando\n", req.Sensor)
									// fila vem no próximo passo
									return
								}
								repo.RicartInstance.IniciarRequisicao(drone.ID, req)
							}()
						}
					case "DESPACHAR_DRONE":
						dadosJSON, _ := json.Marshal(mensagem.Dados)
						var missao models.MensagemDrone
						if err := json.Unmarshal(dadosJSON, &missao); err != nil {
							log.Printf("Erro ao parsear DESPACHAR_DRONE: %v\n", err)
							continue
						}

						log.Printf("[P2P] Recebi ordem externa para despachar MEU drone %s\n", missao.DroneID)

						// Pega a struct, transforma em byte de novo e joga no socket do drone físico
						data, _ := json.Marshal(missao)
						if !repo.EnviarParaDrone(missao.DroneID, data) {
							log.Printf("ERRO: Drone %s não está conectado no servidor para receber a missão\n", missao.DroneID)
						}

					}

				}

				log.Printf("Reconectando com %s...\n", addr)
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
		log.Printf("Peer %s não encontrado ou offline\n", zonaID)
		return
	}

	data, _ := json.Marshal(mensagem)
	peer.Conn.Write(append(data, '\n'))
}

// FUNCAO QUE SIMULAA OS DRONESSSSS
func seedDrones() {
	minhaZona := getZona()
	dronesIniciais := []models.Drone{
		{ID: minhaZona + "-drone-01", Status: models.StatusLivre, ZonaBase: minhaZona, ZonaAtual: minhaZona},
		{ID: minhaZona + "-drone-02", Status: models.StatusLivre, ZonaBase: minhaZona, ZonaAtual: minhaZona},
	}
	for _, d := range dronesIniciais {
		repo.AtualizarDrone(d)
	}
}

// FUNCAO DE BROADCAST ENTRE OS PEEARS
func BroadcastDroneUpdate(drone models.Drone) {
	repo.Mutex.RLock()
	peers := make(map[string]models.Peer)
	for k, v := range repo.Peers {
		peers[k] = v
	}
	repo.Mutex.RUnlock()

	mensagem := models.Mensagem{
		Tipo:      "DRONE_UPDATE",
		De:        getZona(),
		Dados:     drone,
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(mensagem)
	for _, peer := range peers {
		if peer.Alive && peer.Conn != nil {
			peer.Conn.Write(append(data, '\n'))
		}
	}
}

func main() {
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
			return len(repo.Peers)
		},

		AoAlocar: func(droneID string) {
			repo.DroneMutex.Lock()
			drone, existe := repo.Drones[droneID]
			if !existe || drone.Status != models.StatusLivre {
				repo.DroneMutex.Unlock()
				// IMPORTANTE: libera a seção crítica antes de tentar novamente
				// Sem isso, os REPLYs adiados nunca são enviados e os peers ficam travados
				repo.RicartInstance.Liberar(droneID)
				go repo.TentarAlocarDaFila()
				return
			}
			drone.Status = models.StatusOcupado
			drone.ZonaAtual = getZona()
			repo.Drones[droneID] = drone
			repo.DroneMutex.Unlock()

			BroadcastDroneUpdate(drone)
			log.Printf("[MAIN] Drone %s alocado via ricart\n", droneID)

			reqAtual := repo.RicartInstance.RequisicaoAtual
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

			// TENTA ENVIAR DIRETO (se estiver conectado no meu socket TCP)
			data, _ := json.Marshal(missao)
			if repo.EnviarParaDrone(droneID, data) {
				log.Printf("[MAIN] Missão enviada DIRETAMENTE para o drone local %s\n", droneID)
				// Drone local: Liberar é chamado quando MISSAO_CONCLUIDA chegar via processarDrone
			} else {
				// DRONE REMOTO: despacha e já libera a seção crítica.
				// A zona hospedeira cuidará do MISSAO_CONCLUIDA e enviará RELEASE via seu próprio Liberar.
				// Se não liberarmos aqui, ficamos NA_SECAO para sempre esperando um evento que nunca chega.
				log.Printf("[MAIN] Drone %s é remoto. Repassando comando para a zona %s\n", droneID, drone.ZonaBase)
				enviarParaZona(drone.ZonaBase, models.Mensagem{
					Tipo:  "DESPACHAR_DRONE",
					De:    getZona(),
					Para:  drone.ZonaBase,
					Dados: missao,
				})
				repo.RicartInstance.Liberar(droneID)
			}
		},

		AoFalharAlocacao: func() {
			// Não faz mais nada — AoAlocar já chama Liberar + TentarAlocarDaFila
			// Este callback só existe para compatibilidade
			log.Printf("[MAIN] AoFalharAlocacao chamado (não deve mais ocorrer)\n")
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
		TentarAlocar: func() {
			time.Sleep(200 * time.Millisecond) // pequeno delay para o DRONE_UPDATE chegar antes
			repo.TentarAlocarDaFila()
		},
	}

	// Configura função de broadcast para o Ricart
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

	// Enviar dados customizados periodicamente ////////////////////////////////////////////////////////////////
	// go func() {
	// 	minhaZona := getZona()
	// 	ticker := time.NewTicker(10 * time.Second)
	// 	defer ticker.Stop()

	// 	for range ticker.C {
	// 		mensagem := models.Mensagem{
	// 			Tipo:      "DATA",
	// 			De:        minhaZona,
	// 			Para:      "", // broadcast
	// 			Dados:     map[string]string{"status": "online", "timestamp": time.Now().String()},
	// 			Timestamp: time.Now(),
	// 		}
	// 		enviarParaTodos(mensagem)
	// 	}
	// }()

	// // TESTE ALOCACAO DE DRONES
	// go func() {
	// 	time.Sleep(30 * time.Second)
	// 	ticker := time.NewTicker(25 * time.Second)
	// 	defer ticker.Stop()
	// 	for range ticker.C {
	// 		drone, ok := repo.SelecionarDroneLivre()
	// 		if !ok {
	// 			log.Printf("[TEST] Nenhum drone livre disponível\n")
	// 			continue
	// 		}
	// 		log.Printf("[TEST] Drone livre encontrado: %s — iniciando requisição\n", drone.ID)
	// 		repo.RicartInstance.IniciarRequisicao(drone.ID)
	// 	}
	// }()

	// debug ///////////////////////////////////////////////////////////////////////////
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Mostra drones locais
			drones := repo.BuscarDrones()
			log.Printf("\n========= DRONES LOCAIS [%s] =========\n", getZona())
			for _, d := range drones {
				log.Printf("  ID: %s | Status: %s | Base: %s | Atual: %s\n",
					d.ID, d.Status, d.ZonaBase, d.ZonaAtual)
			}
			log.Printf("======================================\n")

			// Pede para cada peer
			enviarParaTodos(models.Mensagem{
				Tipo:      "GET_DRONES",
				De:        getZona(),
				Timestamp: time.Now(),
			})
		}
	}()

	// Abrir servidor para escutar conexoes
	listner, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("TCP rodando na porta 9090")

	for {
		conn, err := listner.Accept()
		if err != nil {
			continue
		}

		go handler.ProcessarConexoes(conn)
	}

}