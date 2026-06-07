package blockchain

import (
	"encoding/json"
	"fmt"
	"log"
	"pbl-2/zona/models"
	"strings"
	"sync"
	"time"
)

// CONSENSUS LOGIC =================================================================

var PendingConsensus sync.Map // Hash (string) -> chan bool

func ValidarBlocoCandidato(b Bloco, c *Chain) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.blocos) > 0 {
		ultimo := c.blocos[len(c.blocos)-1]
		if b.Index != ultimo.Index+1 {
			if b.Index != ultimo.Index {
				return fmt.Errorf("índice inválido: esperado %d ou %d, recebido %d", ultimo.Index, ultimo.Index+1, b.Index)
			}
			// Disputa pelo mesmo slot: menor hash vence
			if b.Hash >= ultimo.Hash {
				return fmt.Errorf("bloco candidato perde na disputa lexicográfica")
			}
		} else {
			if b.HashAnterior != ultimo.Hash {
				return fmt.Errorf("hash anterior não confere")
			}
		}
	} else if b.Index != 0 {
		return fmt.Errorf("primeiro bloco deve ter índice 0")
	}

	if b.Hash != HashBloco(b) {
		return fmt.Errorf("hash inválido")
	}

	if !strings.HasPrefix(b.Hash, "00") {
		return fmt.Errorf("PoW insuficiente")
	}

	// Verificação de saldo se for débito
	if b.Tipo == TipoCredito {
		var dados DadosCredito
		bytes, _ := json.Marshal(b.Dados)
		json.Unmarshal(bytes, &dados)
		if dados.Valor < 0 {
			saldo := 0.0
			for _, bloc := range c.blocos {
				if bloc.Tipo == TipoCredito {
					var d DadosCredito
					bt, _ := json.Marshal(bloc.Dados)
					json.Unmarshal(bt, &d)
					if d.Zona == dados.Zona {
						saldo += d.Valor
					}
				}
			}
			if saldo+dados.Valor < 0 {
				return fmt.Errorf("saldo insuficiente para zona %s", dados.Zona)
			}
		}
	}

	return nil
}

func ProposeAndMine(
	tipo TipoBloco,
	dados any,
	chain *Chain,
	enviarParaTodos func(models.Mensagem),
	totalPeers func() int,
	minhaZona string,
) bool {
	bloco := MineBloco(tipo, dados, chain)
	respostas := make(chan bool, totalPeers()+1)
	
	PendingConsensus.Store(bloco.Hash, respostas)
	defer PendingConsensus.Delete(bloco.Hash)

	log.Printf("[CONSENSUS] 🗳 Propondo bloco #%d (%s) - hash: %s\n", bloco.Index, tipo, bloco.Hash[:12])

	enviarParaTodos(models.Mensagem{
		Tipo: models.TipoProposeBlock,
		De:   minhaZona,
		Dados: models.MensagemBloco{
			Bloco: bloco,
		},
		Timestamp: time.Now(),
	})

	aceitos := 1 // auto-accept
	total := totalPeers() + 1
	quorum := (total * 2 / 3)
	if quorum <= 0 { quorum = 1 }

	timer := time.After(5 * time.Second)
	
	for i := 0; i < total-1; i++ {
		select {
		case ok := <-respostas:
			if ok {
				aceitos++
			}
		case <-timer:
			log.Printf("[CONSENSUS] ⏱ Timeout! Assumindo ACCEPT implícito para peers restantes.\n")
			aceitos = total 
			goto finaliza
		}
		if aceitos >= quorum {
			goto finaliza
		}
	}

finaliza:
	if aceitos < quorum {
		log.Printf("[CONSENSUS] ✗ Bloco rejeitado: quorum não atingido (%d/%d)\n", aceitos, quorum)
		return false
	}

	log.Printf("[CONSENSUS] ✔ Bloco aceito! (%d/%d)\n", aceitos, quorum)
	err := chain.AppendBloco(bloco)
	if err != nil {
		log.Printf("[CONSENSUS] ✗ Erro ao appendar: %v\n", err)
		return false
	}

	enviarParaTodos(models.Mensagem{
		Tipo: models.TipoNewBlock,
		De:   minhaZona,
		Dados: models.MensagemBloco{
			Bloco: bloco,
		},
		Timestamp: time.Now(),
	})

	return true
}
