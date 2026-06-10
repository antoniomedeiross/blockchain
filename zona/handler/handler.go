package handler

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"pbl-2/zona/ledger"
	"pbl-2/zona/models"
	"pbl-2/zona/repo"
	"strings"
	"time"
)

func ProcessarConexoes(conn net.Conn) {
	defer conn.Close()

	addr := conn.RemoteAddr().String()
	log.Println("Cliente TCP conectado:", addr)

	leitor := bufio.NewReader(conn)
	var peerZona string

	for {
		msg, err := leitor.ReadString('\n')

		// Se der erro lendo, pode ser que o peer tenha caído ou que seja um sensor desconectando (normal) — trata ambos os casos
		if err != nil {
			if peerZona != "" {
				log.Printf("[P2P] ✗ Peer %s desconectado\n", peerZona)
				repo.Mutex.Lock()

				// Se ta na lista de peers, marca como offline (mas não remove pra tentar reconectar depois)
				if peer, exists := repo.Peers[peerZona]; exists {
					peer.Alive = false
					repo.Peers[peerZona] = peer
				}
				repo.Mutex.Unlock()
				// Notifica o Ricart imediatamente — se estávamos esperando REPLY
				// deste peer, não precisamos esperar o watchdog
				repo.RicartInstance.NotificarPeerOffline(peerZona)
			}
			// sensor desconectou — normal, não loga nada
			return
		}

		msg = strings.TrimSpace(msg)

		if msg == "" {
			continue
		}

		// Protocolo: primeira mensagem deve ser identificação (IAM:ZONA/SENSOR/DRONE)
		if strings.HasPrefix(msg, "IAM:") {
			partes := strings.SplitN(strings.TrimPrefix(msg, "IAM:"), ":", 3)

			// SE FOR UN SENSOR
			if partes[0] == "SENSOR" {
				// É um sensor — só confirma e processa requisições
				sensorID := partes[1]
				log.Printf("[SENSOR] ✔ Sensor conectado: %s\n", sensorID)
				conn.Write([]byte("OK\n"))
				// peerZona fica vazio — sensor não é peer
				continue
			}

			// SE FOR UM DRONE
			if partes[0] == "DRONE" {
				droneID := partes[1]
				log.Printf("[DRONE] ✔ Drone conectado: %s\n", droneID)

				// Registra conexão do drone para poder enviar missões depois
				repo.RegistrarConexaoDrone(droneID, conn)

				repo.DroneMutex.Lock()
				// Verifica se o drone já existe e tem uma zona base registrada
				droneExistente, jaConhecido := repo.Drones[droneID]
				if jaConhecido && droneExistente.ZonaBase != "" {
					// Drone reconectando (failover): preserva ZonaBase original, atualiza ZonaAtual
					// para refletir a zona onde está fisicamente conectado agora
					droneExistente.Status = models.StatusLivre
					droneExistente.ZonaAtual = getZonaAtual() // zona deste peer, não a base original
					droneExistente.MissaoAtual = nil
					repo.Drones[droneID] = droneExistente
					log.Printf("\n[FAILOVER] ══► Drone %s reconectou via failover (base: %s → agora em: %s)\n",
						droneID, droneExistente.ZonaBase, getZonaAtual())
				} else {
					// Drone novo: registra pela primeira vez
					repo.Drones[droneID] = models.Drone{
						ID:        droneID,
						Status:    models.StatusLivre,
						ZonaBase:  getZonaAtual(),
						ZonaAtual: getZonaAtual(),
					}
				}
				repo.DroneMutex.Unlock()

				// Notifica todos os peers sobre o novo drone ou a reconexão
				repo.BroadcastFn(repo.Drones[droneID])
				conn.Write([]byte("OK\n"))

				// Se há missões pendentes na fila, tenta despachar agora que temos um drone disponível
				go func() {
					repo.TentarAlocarDaFila()
				}()

				// Bloqueia aqui até o drone desconectar
				processarDrone(droneID, conn, leitor)

				return // só sai depois que o drone cair
			}

			// SE FOR UM PEER (NOVO FLUXO)
			if partes[0] == "PEER" {
				peerZona = partes[1]
				listenAddr := partes[2] // Aqui está o IP real de escuta! (Ex: peer1:9090)

				log.Printf("[P2P] ✔ Peer %s conectado (escutando em %s)\n", peerZona, listenAddr)

				repo.Mutex.Lock()
				repo.Peers[peerZona] = models.Peer{
					ZoneID:   peerZona,
					Address:  listenAddr, // <-- O SEGREDO: Salva a porta certa, não a efêmera
					Alive:    true,
					LastSeen: time.Now(),
					Conn:     conn,
				}
				repo.Mutex.Unlock()

				conn.Write([]byte("OK\n"))
				continue
			}
		}

		// Mensagens subsequentes (JSON)
		var mensagem models.Mensagem
		err = json.Unmarshal([]byte(msg), &mensagem)
		if err != nil {
			log.Printf("Erro ao desserializar mensagem de %s: %v\n", peerZona, err)
			continue
		}

		// Atualizar timestamp do peer
		repo.Mutex.Lock()
		if peer, exists := repo.Peers[peerZona]; exists {
			peer.LastSeen = time.Now()
			repo.Peers[peerZona] = peer
		}
		repo.Mutex.Unlock()

		log.Printf("[%s] ← %s de: %s\n", peerZona, mensagem.Tipo, mensagem.De)

		// Processar tipos de mensagem
		switch mensagem.Tipo {
		case "HEARTBEAT":
			// Responder com HEARTBEAT
			resposta := models.Mensagem{
				Tipo:      "HEARTBEAT",
				De:        getZonaAtual(),
				Para:      peerZona,
				Timestamp: time.Now(),
			}
			if data, err := json.Marshal(resposta); err == nil {
				conn.Write(append(data, '\n'))
			}

		case "DATA":
			// Nao precisei usar

		case "STATUS":
			// Responder com status do servidor
			resposta := models.Mensagem{
				Tipo:      "STATUS",
				De:        getZonaAtual(),
				Para:      peerZona,
				Dados:     map[string]interface{}{"status": "online", "peers": len(repo.Peers)},
				Timestamp: time.Now(),
			}
			if data, err := json.Marshal(resposta); err == nil {
				conn.Write(append(data, '\n'))
			}

		case "BLOCO":
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var bloco ledger.Bloco
			if err := json.Unmarshal(dadosJSON, &bloco); err != nil {
				log.Printf("Erro ao parsear BLOCO: %v\n", err)
				continue
			}
			ledger.AceitarBlocoExterno(bloco)

		case "REQUISICAO_DRONE":
			// Recebi uma requisição de drone de um peer — enfileira e tenta alocar
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var req models.Requisicao
			if err := json.Unmarshal(dadosJSON, &req); err != nil {
				log.Printf("Erro ao parsear REQUISICAO_DRONE: %v\n", err)
				continue
			}

			log.Printf("[FILA] ↓ Nova req de %s enfileirada — prioridade=%d (%s)\n", req.Sensor, req.Prioridade, req.Ocorrencia)

			// Coloca na fila de prioridade
			repo.Enfileirar(req)

			// Tenta processar. Se o Ricart estiver ocupado, ela morre e a fila guarda o pedido.
			go repo.TentarAlocarDaFila()

		case "DESPACHAR_DRONE":
			// Outra zona alocou MEU drone e me pediu para entregar a missão a ele
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var missao models.MensagemDrone
			if err := json.Unmarshal(dadosJSON, &missao); err != nil {
				continue
			}

			// VALIDAÇÃO DE SEGURANÇA: Verifica se a zona que enviou o DESPACHAR tem saldo
			// no MEU ledger local. Se ela mandou um bloco falso, meu ledger rejeitou,
			// então o saldo dela aqui estará baixo.
			if !ledger.Instancia.TemSaldo(mensagem.De) {
				log.Printf("[SEGURANÇA] ✗ Tentativa de DESPACHAR de %s negada: saldo insuficiente no ledger local\n", mensagem.De)
				continue
			}

			log.Printf("[MISSÃO] ◄ [%s] DESPACHAR drone físico %s\n", peerZona, missao.DroneID)

			// Agora sim, o conn.Write vai funcionar, porque o drone tá conectado aqui!
			data, _ := json.Marshal(missao)
			if !repo.EnviarParaDrone(missao.DroneID, data) {
				log.Printf("[MISSÃO] ✗ ERRO: Drone %s não está conectado localmente\n", missao.DroneID)
			}

			
		case "SYNC_REQUEST":
			// Peer conectou e quer o estado atual dos drones E possivelmente blocos da ledger
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var payload map[string]interface{}
			json.Unmarshal(dadosJSON, &payload)

			peerChainSize := 0
			if val, ok := payload["chain_size"].(float64); ok {
				peerChainSize = int(val)
			}

			// 1. Enviar Drones
			drones := repo.BuscarDrones()
			respostaDrones := models.Mensagem{
				Tipo:      "SYNC_RESPONSE",
				De:        getZonaAtual(),
				Para:      peerZona,
				Dados:     drones,
				Timestamp: time.Now(),
			}
			if data, err := json.Marshal(respostaDrones); err == nil {
				conn.Write(append(data, '\n'))
			}

			// 2. Enviar blocos que o peer não tem (Catch-up)
			if ledger.Instancia != nil {
				meuTamanho := ledger.Instancia.Tamanho()
				if meuTamanho > peerChainSize {
					log.Printf("[SYNC] → Peer %s está atrasado (%d vs %d). Enviando %d bloco(s).\n",
						peerZona, peerChainSize, meuTamanho, meuTamanho-peerChainSize)
					
					fullChain := ledger.Instancia.Snapshot()
					blocosFaltantes := fullChain[peerChainSize:]
					
					for _, b := range blocosFaltantes {
						msgBloco := models.Mensagem{
							Tipo:  "BLOCO",
							De:    getZonaAtual(),
							Dados: b,
						}
						if data, err := json.Marshal(msgBloco); err == nil {
							conn.Write(append(data, '\n'))
						}
					}
				}
			}
			log.Printf("[SYNC] → Enviado estado para %s: %d drone(s)\n", peerZona, len(drones))

		case "SYNC_RESPONSE":
			// Recebi o estado completo de outro peer, atualizo minha lista
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var drones map[string]models.Drone
			if err := json.Unmarshal(dadosJSON, &drones); err != nil {
				log.Printf("Erro ao parsear SYNC_RESPONSE: %v\n", err)
				continue
			}
			for _, d := range drones {
				repo.AtualizarDroneRemoto(d)
			}
			log.Printf("[SYNC] ✔ Estado recebido de %s: %d drone(s)\n", peerZona, len(drones))

		case "DRONE_UPDATE":
			// Um peer atualizou o estado de um drone
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var drone models.Drone
			if err := json.Unmarshal(dadosJSON, &drone); err != nil {
				log.Printf("Erro ao parsear DRONE_UPDATE: %v\n", err)
				continue
			}
			repo.AtualizarDroneRemoto(drone)
			log.Printf("[ESTADO] Drone %s → %s (notificado por %s)\n", drone.ID, drone.Status, peerZona)

		case "GET_DRONES":
			// Peer quer a lista atual de drones para sincronizar
			drones := repo.BuscarDrones()
			resposta := models.Mensagem{
				Tipo:      "DRONES_RESPONSE",
				De:        getZonaAtual(),
				Para:      peerZona,
				Dados:     drones,
				Timestamp: time.Now(),
			}
			if data, err := json.Marshal(resposta); err == nil {
				conn.Write(append(data, '\n'))
			}
			log.Printf("[SYNC] → GET_DRONES respondido para %s: %d drone(s)\n", peerZona, len(drones))

		case "DRONES_RESPONSE":
			// Recebi a lista de drones de outro peer
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

		case "REQUEST", "REPLY", "RELEASE":
			// Mensagens do Ricart: só repassa para a instância do Ricart processar

			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var ricartMsg models.MensagemRicart
			if err := json.Unmarshal(dadosJSON, &ricartMsg); err != nil {
				log.Printf("Erro ao parsear mensagem Ricart: %v\n", err)
				continue
			}
			switch mensagem.Tipo {
			case "REQUEST":
				// Recebi um REQUEST de outro peer — passo para o Ricart processar e decidir se libero ou não
				repo.RicartInstance.ReceberRequest(ricartMsg.De, ricartMsg.DroneID, ricartMsg.Timestamp, ricartMsg.Prioridade, ricartMsg.ReqTimestamp)
			case "REPLY":
				// Recebi um REPLY de outro peer — passo para o Ricart processar e ver se já posso entrar na seção crítica
				repo.RicartInstance.ReceberReply(ricartMsg.De, ricartMsg.DroneID)
			case "RELEASE":
				// Recebi um RELEASE de outro peer — passo para o Ricart processar e ver se já posso entrar na seção crítica
				repo.RicartInstance.ReceberRelease(ricartMsg.De, ricartMsg.DroneID)
			}

		case "MISSAO_CONCLUIDA_ACK":
			// Uma zona de failover concluiu a missão de um drone nosso.
			// Precisamos chamar Liberar() aqui, pois fomos nós que iniciamos o Ricart.
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var ack models.AckMissao
			if err := json.Unmarshal(dadosJSON, &ack); err != nil {
				log.Printf("Erro ao parsear MISSAO_CONCLUIDA_ACK: %v\n", err)
				continue
			}
			log.Printf("[FAILOVER] ✔ Drone %s concluiu missão em %s — Ricart liberado\n", ack.DroneID, peerZona)
			
			repo.RicartInstance.Liberar(ack.DroneID)
			
			// Tenta puxar a próxima requisição da fila depois que um drone fica livre
			go func() {
				time.Sleep(300 * time.Millisecond)
				repo.TentarAlocarDaFila()
			}()

		case "FORCE_RELEASE":
			// Caso de emergência: se uma zona reiniciar, ela pode enviar isso para
			// avisar que não está mais segurando nenhum drone.
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var droneID string
			json.Unmarshal(dadosJSON, &droneID)
			log.Printf("[RICART] ⚠ FORCE_RELEASE recebido de %s para drone %s\n", mensagem.De, droneID)
			repo.RicartInstance.ReceberRelease(mensagem.De, droneID)

		default:
			log.Printf("[HANDLER] ⚠ Tipo de mensagem desconhecido: %s\n", mensagem.Tipo)
		}
	}
}

// retorna o nome da zona atual
func getZonaAtual() string {
	zona := os.Getenv("ZONA")
	if zona == "" {
		return "DESCONHECIDA"
	}
	return zona
}

// FUNCOES =====================================================================

// processar as mensagens de um drone específico até ele desconectar (missão concluída ou falha)
func processarDrone(droneID string, conn net.Conn, leitor *bufio.Reader) {
	defer func() {
		log.Printf("[DRONE] ✗ Drone %s desconectado\n", droneID)
		repo.RemoverConexaoDrone(droneID)

		repo.DroneMutex.Lock()
		d, ok := repo.Drones[droneID]
		if ok {
			missaoInterrompida := d.MissaoAtual // salva antes de limpar
			d.Status = models.StatusOffline
			d.MissaoAtual = nil
			repo.Drones[droneID] = d
			repo.DroneMutex.Unlock()

			// Se havia missão em andamento, recoloca na fila com prioridade original
			if missaoInterrompida != nil {
				log.Printf("\n[DRONE] ⚠ Drone %s CAIU durante missão '%s' (prioridade %d) — recolocando na fila\n",
					droneID, missaoInterrompida.Ocorrencia, missaoInterrompida.Prioridade)
				repo.Enfileirar(*missaoInterrompida)
			}

			// Se o Ricart estava NA_SECAO com este drone, libera para não travar os peers
			repo.RicartInstance.Mu.Lock()
			if repo.RicartInstance.Estado == models.EstadoNaSecao &&
				repo.RicartInstance.DroneAlvo == droneID {
				repo.RicartInstance.Mu.Unlock()
				repo.RicartInstance.Liberar(droneID)
			} else {
				repo.RicartInstance.Mu.Unlock()
			}

			repo.BroadcastFn(d)
		} else {
			repo.DroneMutex.Unlock()
		}
		conn.Close()
	}()

	for {
		msg, err := leitor.ReadString('\n')
		if err != nil {
			return
		}
		msg = strings.TrimSpace(msg)
		if msg == "" {
			continue
		}

		var mensagem models.MensagemDrone
		if err := json.Unmarshal([]byte(msg), &mensagem); err != nil {
			continue
		}

		// Processar tipos de mensagem do drone
		switch mensagem.Tipo {
		// Quando o drone conclui a missão, precisamos atualizar o estado local, notificar os peers e liberar o Ricart
		case "MISSAO_CONCLUIDA":
			log.Printf("[DRONE] ✔ Drone %s concluiu missão — liberando\n", droneID)
			// Atualiza estado localmente
			repo.RemoverGerenciamento(droneID)

			repo.DroneMutex.Lock()
			// Verifica se o drone é da própria zona ou de failover (reconectou aqui, mas a base é outra)
			d := repo.Drones[droneID]

			// REGISTRO DE LAUDO (IMUTABILIDADE E AUDITORIA)
			if d.MissaoAtual != nil {
				ledger.RegistrarLaudo(d.MissaoAtual.ZonaID, droneID, d.MissaoAtual.Ocorrencia, d.ZonaAtual)
			}

			d.Status = models.StatusLivre
			d.MissaoAtual = nil

			// Se o drone é de failover, ele vai estar conectado aqui, mas a zona base é outra. Nesse caso, não atualizamos ZonaAtual
			minhaZona := getZonaAtual()
			droneELocal := d.ZonaBase == minhaZona
			zonaBase := d.ZonaBase

			if droneELocal {
				// Drone da própria zona: retorna para base normalmente
				d.ZonaAtual = d.ZonaBase
			} else {
				// Drone em failover: continua na zona atual (está conectado fisicamente aqui)
				d.ZonaAtual = minhaZona
			}
			// Atualiza o mapa de drones
			repo.Drones[droneID] = d
			repo.DroneMutex.Unlock()

			// Avisa todos os peers que o drone está livre
			repo.BroadcastFn(d)

			if droneELocal {
				// Drone local -> libera o Ricart normalmente
				repo.RicartInstance.Liberar(droneID)
			} else {
				// Drone de failover: a zona base foi quem iniciou o Ricart,
				// então ela que deve chamar Liberar(). Enviamos MISSAO_CONCLUIDA_ACK para ela.
				log.Printf("[FAILOVER] → Notificando zona base %s para liberar Ricart do drone %s\n", zonaBase, droneID)
				repo.NotificarMissaoConcluida(zonaBase, droneID)
			}

			// Tenta puxar a próxima requisição da fila
			go func() {
				time.Sleep(500 * time.Millisecond)
				repo.TentarAlocarDaFila()
			}()

		// quando conecta o drone pede a lista de peers para saber para onde ir quando cair
		case "GET_PEERS_LIST":
			repo.Mutex.RLock()
			var lista []string
			for _, peer := range repo.Peers {
				if peer.Alive {
					lista = append(lista, peer.Address)
				}
			}
			repo.Mutex.RUnlock()

			// Adiciona o próprio broker na lista usando a variável de rede correta
			meuAddr := os.Getenv("MY_ADDR")
			lista = append(lista, meuAddr)

			resposta := models.MensagemDrone{
				Tipo:  "PEERS_LIST_RESPONSE",
				De:    getZonaAtual(),
				Dados: lista,
			}
			data, _ := json.Marshal(resposta)
			conn.Write(append(data, '\n'))
		}
	}
}
