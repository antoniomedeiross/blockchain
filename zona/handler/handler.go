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

			if partes[0] == "SENSOR" {
				// É um sensor — só confirma e processa requisições
				sensorID := partes[1]
				log.Printf("Sensor conectado: %s\n", sensorID)
				conn.Write([]byte("OK\n"))
				// peerZona fica vazio — sensor não é peer
				continue
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
			log.Printf("[ZONA] Requisição do sensor %s — ocorrência: %s, prioridade: %d\n",
				req.Sensor, req.Ocorrencia, req.Prioridade)

			go func() {
				drone, ok := repo.SelecionarDroneLivre()
				if !ok {
					log.Printf("[ZONA] Nenhum drone livre para sensor %s\n", req.Sensor)
					return
				}
				repo.RicartInstance.IniciarRequisicao(drone.ID)
			}()

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
