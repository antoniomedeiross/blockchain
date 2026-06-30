package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

const ArquivoLedger = "ledger.json"

// SalvarBloco grava um único bloco no final do arquivo JSON
func SalvarBloco(bloco Bloco) error {
	f, err := os.OpenFile(ArquivoLedger, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("erro ao abrir arquivo ledger: %v", err)
	}
	defer f.Close()

	// Serializa o bloco em uma linha
	dados, err := json.Marshal(bloco)
	if err != nil {
		return fmt.Errorf("erro ao serializar bloco: %v", err)
	}

	if _, err := f.Write(append(dados, '\n')); err != nil {
		return fmt.Errorf("erro ao escrever no arquivo: %v", err)
	}

	return nil
}

// CarregarLedger lê todos os blocos do arquivo e reconstrói a chain
func CarregarLedger() ([]Bloco, error) {
	if _, err := os.Stat(ArquivoLedger); os.IsNotExist(err) {
		return nil, nil // arquivo não existe ainda, começa do zero
	}

	f, err := os.Open(ArquivoLedger)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var blocos []Bloco
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var bloco Bloco
		if err := json.Unmarshal(scanner.Bytes(), &bloco); err != nil {
			log.Printf("[LEDGER] ⚠ Erro ao ler linha do arquivo: %v\n", err)
			continue
		}
		blocos = append(blocos, bloco)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return blocos, nil
}

// ResetarArquivo apaga o conteúdo do ledger (usado se precisar limpar o estado)
func ResetarArquivo() {
	os.Remove(ArquivoLedger)
}
