package handler

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
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
		if err != nil {
			if peerZona != "" {
				log.Printf("Peer %s desconectado\n", peerZona)
				repo.Mutex.Lock()
				if peer, exists := repo.Peers[peerZona]; exists {
					peer.Alive = false
					repo.Peers[peerZona] = peer
				}
				repo.Mutex.Unlock()
			}
			// sensor desconectou — normal, não loga nada
			return
		}

		msg = strings.TrimSpace(msg)

		if msg == "" {
			continue
		}

		// Protocolo: primeira mensagem deve ser identificação (IAM:ZONA)
		if strings.HasPrefix(msg, "IAM:") {
			partes := strings.SplitN(strings.TrimPrefix(msg, "IAM:"), ":", 2)

			// SE FOR UN SENSOR
			if partes[0] == "SENSOR" {
				// É um sensor — só confirma e processa requisições
				sensorID := partes[1]
				log.Printf("Sensor conectado: %s\n", sensorID)
				conn.Write([]byte("OK\n"))
				// peerZona fica vazio — sensor não é peer
				continue
			}

			// SE FOR UM DRONE
			if partes[0] == "DRONE" {
				droneID := partes[1]
				log.Printf("Drone conectado: %s\n", droneID)

				repo.RegistrarConexaoDrone(droneID, conn)

				repo.DroneMutex.Lock()
				repo.Drones[droneID] = models.Drone{
					ID:        droneID,
					Status:    models.StatusLivre,
					ZonaBase:  getZonaAtual(),
					ZonaAtual: getZonaAtual(),
				}
				repo.DroneMutex.Unlock()

				repo.BroadcastFn(repo.Drones[droneID])
				conn.Write([]byte("OK\n"))

				// Bloqueia aqui até o drone desconectar — NÃO usa goroutine
				processarDrone(droneID, conn, leitor)
				return // só sai depois que o drone cair
			}

			// É uma zona — fluxo normal
			peerZona = partes[0]
			log.Printf("Peer identificado como zona: %s (de %s)\n", peerZona, addr)

			repo.Mutex.Lock()
			repo.Peers[peerZona] = models.Peer{
				ZoneID:   peerZona,
				Address:  addr,
				Alive:    true,
				LastSeen: time.Now(),
				Conn:     conn,
			}
			repo.Mutex.Unlock()

			conn.Write([]byte("OK\n"))
			continue
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

		log.Printf("[%s] Mensagem recebida - Tipo: %s, De: %s, Para: %s\n", peerZona, mensagem.Tipo, mensagem.De, mensagem.Para)

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
			// Processar dados recebidos
			log.Printf("[%s] Dados: %v\n", peerZona, mensagem.Dados)
			// Aqui você pode processar os dados como desejar

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

		case "REQUISICAO_DRONE":
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var req models.Requisicao
			if err := json.Unmarshal(dadosJSON, &req); err != nil {
				log.Printf("Erro ao parsear REQUISICAO_DRONE: %v\n", err)
				continue
			}
			log.Printf("[ZONA] Nova requisição do sensor %s ENFILEIRADA — prioridade: %d\n", req.Sensor, req.Prioridade)

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

			log.Printf("[%s] Recebi ordem externa para despachar MEU drone físico %s\n", peerZona, missao.DroneID)

			// Agora sim, o conn.Write vai funcionar, porque o drone tá conectado aqui!
			data, _ := json.Marshal(missao)
			if !repo.EnviarParaDrone(missao.DroneID, data) {
				log.Printf("ERRO CRÍTICO: Não consegui repassar missão ao drone local %s\n", missao.DroneID)
			}

		case "SYNC_REQUEST":
			// Peer conectou e quer o estado atual dos drones
			drones := repo.BuscarDrones()
			resposta := models.Mensagem{
				Tipo:      "SYNC_RESPONSE",
				De:        getZonaAtual(),
				Para:      peerZona,
				Dados:     drones,
				Timestamp: time.Now(),
			}
			if data, err := json.Marshal(resposta); err == nil {
				conn.Write(append(data, '\n'))
			}
			log.Printf("[%s] SYNC_RESPONSE enviado com %d drones\n", peerZona, len(drones))

		case "SYNC_RESPONSE":
			// Recebi o estado completo de outro peer, atualizo minha lista
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var drones map[string]models.Drone
			if err := json.Unmarshal(dadosJSON, &drones); err != nil {
				log.Printf("Erro ao parsear SYNC_RESPONSE: %v\n", err)
				continue
			}
			for _, d := range drones {
				repo.AtualizarDrone(d)
			}
			log.Printf("[%s] Estado sincronizado: %d drones recebidos\n", peerZona, len(drones))

		case "DRONE_UPDATE":
			// Um peer atualizou o estado de um drone
			dadosJSON, _ := json.Marshal(mensagem.Dados)
			var drone models.Drone
			if err := json.Unmarshal(dadosJSON, &drone); err != nil {
				log.Printf("Erro ao parsear DRONE_UPDATE: %v\n", err)
				continue
			}
			repo.AtualizarDrone(drone)
			log.Printf("[%s] Drone %s atualizado para status: %s\n", peerZona, drone.ID, drone.Status)

		case "GET_DRONES":
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
			log.Printf("[%s] DRONES_RESPONSE enviado com %d drones\n", peerZona, len(drones))

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
			}

		default:
			log.Printf("Tipo de mensagem desconhecido: %s\n", mensagem.Tipo)
		}
	}
}

func getZonaAtual() string {
	zona := os.Getenv("ZONA")
	if zona == "" {
		return "DESCONHECIDA"
	}
	return zona
}

// FUNCOES
func processarDrone(droneID string, conn net.Conn, leitor *bufio.Reader) {
	defer func() {
		log.Printf("Drone %s desconectado\n", droneID)
		repo.RemoverConexaoDrone(droneID)

		// Marca drone como offline e propaga
		repo.DroneMutex.Lock()
		if d, ok := repo.Drones[droneID]; ok {
			d.Status = models.StatusOffline
			repo.Drones[droneID] = d
		}
		repo.DroneMutex.Unlock()
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

		switch mensagem.Tipo {
		case "MISSAO_CONCLUIDA":
			log.Printf("[DRONE %s] Missão concluída — liberando\n", droneID)

			repo.DroneMutex.Lock()
			d := repo.Drones[droneID]
			d.Status = models.StatusLivre // Atualiza estado local
			d.ZonaAtual = d.ZonaBase
			repo.Drones[droneID] = d
			repo.DroneMutex.Unlock()

			// IMPORTANTE: Você precisa avisar os outros peers que esse drone está livre!
			repo.BroadcastFn(d)

			// Agora sim, libera o Ricart-Agrawala para responder os REPLYs pendentes
			repo.RicartInstance.Liberar(droneID)

			// 2. Avisa toda a rede que o drone está livre
			repo.BroadcastFn(d)

			// 3. Libera o lock distribuído no Ricart-Agrawala
			repo.RicartInstance.Liberar(droneID)

			// 4. A MÁGICA AQUI: Tenta puxar a próxima requisição da fila!
			// Como o handler já conhece o repo, não há erro de importação.
			go func() {
				// Delay rápido para dar tempo de o "livre" chegar nos outros brokers via TCP
				time.Sleep(500 * time.Millisecond)
				repo.TentarAlocarDaFila()
			}()

		case "GET_PEERS_LIST":
			// DRONE QUER A LISTA DE PEERS ATIVOS PARA SE COMUNICAR DIRETAMENTE COM ELES (RICART-AGRAWALA)
			repo.Mutex.RLock()
			var lista []string
			for _, peer := range repo.Peers {
				if peer.Alive {
					lista = append(lista, peer.Address)
				}
			}
			repo.Mutex.RUnlock()

			// Adiciona o próprio broker na lista também
			lista = append(lista, getZonaAtual()+":9090") // Ajuste conforme seu IP interno

			resposta := models.Mensagem{
				Tipo:  "PEERS_LIST_RESPONSE",
				De:    getZonaAtual(),
				Dados: lista,
			}
			data, _ := json.Marshal(resposta)
			conn.Write(append(data, '\n'))
		}
	}
}
