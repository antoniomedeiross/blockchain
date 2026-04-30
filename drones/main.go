package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var listaServidores []string

type MensagemDrone struct {
	Tipo      string      `json:"tipo"`
	De        string      `json:"de"`
	DroneID   string      `json:"drone_id"`
	Dados     interface{} `json:"dados,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

type Missao struct {
	RequisicaoID string `json:"requisicao_id"`
	Ocorrencia   string `json:"ocorrencia"`
	Prioridade   int    `json:"prioridade"`
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func tempoMissao(prioridade int) time.Duration {
	tempos := map[int]time.Duration{
		0: 2 * time.Second,
		1: 4 * time.Second,
		2: 7 * time.Second,
		3: 10 * time.Second,
		4: 13 * time.Second,
		5: 15 * time.Second,
	}
	if t, ok := tempos[prioridade]; ok {
		return t
	}
	return 10 * time.Second
}

func main() {
    droneID := getEnv("DRONE_ID", "drone-desconhecido")
    servidorInicial := getEnv("SERVIDOR", "localhost:9001")

    // Inicializa a lista com o servidor que veio do Docker Compose
    listaServidores = []string{servidorInicial}
    indiceServidor := 0

    log.Printf("[DRONE %s] Iniciando...\n", droneID)
    time.Sleep(5 * time.Second)

    for {
        // Proteção: garante que o índice não estoure o tamanho da lista
        if len(listaServidores) == 0 {
            listaServidores = append(listaServidores, servidorInicial)
        }
        indiceServidor = indiceServidor % len(listaServidores)
        
        // AGORA SIM: Pegamos o servidor atual da roleta!
        servidorAtual := listaServidores[indiceServidor]

        // Conecta
        conn, err := net.DialTimeout("tcp", servidorAtual, 5*time.Second)
        if err != nil {
            log.Printf("[DRONE %s] Servidor %s offline. Passando para o próximo...\n", droneID, servidorAtual)
            indiceServidor++ // Roda a roleta
            time.Sleep(3 * time.Second)
            continue
        }

        log.Printf("[DRONE %s] Conectado em %s\n", droneID, servidorAtual)

        // Identifica
        conn.Write([]byte("IAM:DRONE:" + droneID + "\n"))
        leitor := bufio.NewReader(conn)
        resp, _ := leitor.ReadString('\n')
        if strings.TrimSpace(resp) != "OK" {
            log.Printf("[DRONE %s] Identificação falhou\n", droneID)
            conn.Close()
            indiceServidor++ // Roda a roleta se falhar
            time.Sleep(3 * time.Second)
            continue
        }

        log.Printf("[DRONE %s] Identificado com sucesso\n", droneID)

        // 1. Pede a lista
        pedido := MensagemDrone{Tipo: "GET_PEERS_LIST", De: droneID}
        dataPedido, _ := json.Marshal(pedido)
        conn.Write(append(dataPedido, '\n'))

        // 2. Lê a resposta
        respLista, _ := leitor.ReadString('\n')
        var msgLista MensagemDrone
        json.Unmarshal([]byte(respLista), &msgLista)

        if msgLista.Tipo == "PEERS_LIST_RESPONSE" {
            // 3. Atualiza a sua listaServidores interna
            dadosBytes, _ := json.Marshal(msgLista.Dados)
            var novosServidores []string
            json.Unmarshal(dadosBytes, &novosServidores)

            log.Printf("[DRONE %s] Lista de failover atualizada dinamicamente: %v", droneID, novosServidores)
            listaServidores = novosServidores
            indiceServidor = 0 // Reseta o índice para começar a buscar da nova lista se cair
        }

        // Loop de missões
        desconectou := false
        for !desconectou {
            msg, err := leitor.ReadString('\n')
            if err != nil {
                log.Printf("[DRONE %s] Conexão perdida com %s...\n", droneID, servidorAtual)
                conn.Close()
                desconectou = true
                break
            }

            msg = strings.TrimSpace(msg)
            if msg == "" {
                continue
            }

            var mensagem MensagemDrone
            if err := json.Unmarshal([]byte(msg), &mensagem); err != nil {
                log.Printf("[DRONE %s] Erro ao deserializar: %v\n", droneID, err)
                continue
            }

            switch mensagem.Tipo {
            case "MISSAO":
                dadosJSON, _ := json.Marshal(mensagem.Dados)
                var missao Missao
                if err := json.Unmarshal(dadosJSON, &missao); err != nil {
                    log.Printf("[DRONE %s] Erro ao parsear missão: %v\n", droneID, err)
                    continue
                }

                duracao := tempoMissao(missao.Prioridade)
                log.Printf("[DRONE %s] Missão: %s | prioridade: %d | duração: %s\n",
                    droneID, missao.Ocorrencia, missao.Prioridade, duracao)

                time.Sleep(duracao)

                resposta := MensagemDrone{
                    Tipo:      "MISSAO_CONCLUIDA",
                    De:        droneID,
                    DroneID:   droneID,
                    Timestamp: time.Now(),
                }
                data, _ := json.Marshal(resposta)
                conn.Write(append(data, '\n'))
                log.Printf("[DRONE %s] Missão concluída\n", droneID)

            case "PING":
                pong := MensagemDrone{
                    Tipo:      "PONG",
                    De:        droneID,
                    DroneID:   droneID,
                    Timestamp: time.Now(),
                }
                data, _ := json.Marshal(pong)
                conn.Write(append(data, '\n'))
            }
        }

        // Se saiu do loop de missões (desconectou), roda a roleta para a próxima tentativa!
        indiceServidor++
        time.Sleep(3 * time.Second)
    }
}