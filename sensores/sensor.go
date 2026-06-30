package main

import (
	"bufio"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

type Requisicao struct {
	Tipo      string `json:"tipo"`       // "REQUISICAO_DRONE"
	Sensor    string `json:"sensor"`     // ID do sensor
	ZonaID    string `json:"zona_id"`    // zona que ele pertence e que pagará
	Zona      string `json:"zona"`       // zona física onde ocorreu
	Ocorrencia string `json:"ocorrencia"` // tipo de ocorrência
	Prioridade int   `json:"prioridade"` // 0 a 5
	Timestamp time.Time `json:"timestamp"`
}

var ocorrencias = []string{
	"suspeita_bloqueio_rota",
	"falha_sinalizacao",
	"embarcacao_deriva",
	"congestionamento_corredor",
	"objeto_nao_identificado",
	"inspecao_visual_urgente",
	"replanejamento_risco_ambiental",
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func main() {
	sensorID := getEnv("SENSOR_ID", "sensor-desconhecido")
	zona := getEnv("ZONA", "DESCONHECIDA")
	servidorAddr := getEnv("SERVIDOR", "localhost:9090")
	// intervalo em segundos entre requisições
	intervalo := 10

	log.Printf("[SENSOR %s] Iniciando — zona: %s, servidor: %s\n", sensorID, zona, servidorAddr)

	// Aguarda o servidor subir
	time.Sleep(5 * time.Second)

	for {
		// Gera ocorrência aleatória
		ocorrencia := ocorrencias[rand.Intn(len(ocorrencias))]
		prioridade := rand.Intn(6) // 0 a 5

		req := Requisicao{
			Tipo:       "REQUISICAO_DRONE",
			Sensor:     sensorID,
			ZonaID:     zona,
			Zona:       zona,
			Ocorrencia: ocorrencia,
			Prioridade: prioridade,
			Timestamp:  time.Now(),
		}

		enviarRequisicao(servidorAddr, req)

		// Intervalo aleatório entre 10 e 20 segundos
		espera := time.Duration(intervalo+rand.Intn(10)) * time.Second
		log.Printf("[SENSOR %s] Próxima requisição em %s\n", sensorID, espera)
		time.Sleep(espera)
	}
}

// sensor/main.go — atualiza a função enviarRequisicao
func enviarRequisicao(addr string, req Requisicao) {
    conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
    if err != nil {
        log.Printf("[SENSOR] Erro ao conectar: %v\n", err)
        return
    }
    defer conn.Close()

    sensorID := getEnv("SENSOR_ID", "sensor-desconhecido")
    conn.Write([]byte("IAM:SENSOR:" + sensorID + "\n"))

    leitor := bufio.NewReader(conn)
    resp, _ := leitor.ReadString('\n')
    if strings.TrimSpace(resp) != "OK" {
        log.Printf("[SENSOR] Servidor não confirmou identificação\n")
        return
    }

    // Envolve em Mensagem igual ao protocolo do servidor
    mensagem := map[string]interface{}{
        "tipo":      "REQUISICAO_DRONE",
        "de":        sensorID,
        "para":      "",
        "dados":     req,
        "timestamp": time.Now(),
    }

    data, _ := json.Marshal(mensagem)
    conn.Write(append(data, '\n'))

    log.Printf("[SENSOR %s] Requisição enviada — ocorrência: %s, prioridade: %d\n",
        sensorID, req.Ocorrencia, req.Prioridade)
}