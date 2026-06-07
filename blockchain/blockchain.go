package blockchain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type TipoBloco string

const (
	TipoGenesis    TipoBloco = "GENESIS"
	TipoCredito    TipoBloco = "TX_CREDITO"
	TipoLaudoInicio TipoBloco = "LAUDO_INICIO"
	TipoLaudoFinal  TipoBloco = "LAUDO_FINAL"
)

type Bloco struct {
	Index        int       `json:"index"`
	Tipo         TipoBloco `json:"tipo"`
	Timestamp    time.Time `json:"timestamp"`
	Dados        any       `json:"dados"`
	HashAnterior string    `json:"hash_anterior"`
	Hash         string    `json:"hash"`
	Nonce        int       `json:"nonce"`
}

type DadosCredito struct {
	Zona  string  `json:"zona"`
	Valor float64 `json:"valor"` // negativo = debito
	TxID  string  `json:"tx_id"` // UUID para idempotencia
}

type DadosLaudo struct {
	DroneID    string    `json:"drone_id"`
	Zona       string    `json:"zona"`
	Ocorrencia string    `json:"ocorrencia"`
	Prioridade int       `json:"prioridade"`
	Timestamp  time.Time `json:"timestamp"`
}

type Chain struct {
	mu     sync.RWMutex
	blocos []Bloco
}

var ZonasIniciais = []struct {
	Zona  string
	Saldo float64
}{
	{"NORTE", 1000},
	{"SUL", 1000},
	{"LESTE", 1000},
}

func NovaChain() *Chain {
	return &Chain{
		blocos: []Bloco{},
	}
}

func HashBloco(b Bloco) string {
	b.Hash = ""
	dadosJSON, _ := json.Marshal(b.Dados)
	registro := fmt.Sprintf("%d:%s:%s:%s:%s:%d",
		b.Index,
		string(b.Tipo),
		b.Timestamp.Format(time.RFC3339Nano),
		string(dadosJSON),
		b.HashAnterior,
		b.Nonce,
	)
	h := sha256.Sum256([]byte(registro))
	return fmt.Sprintf("%x", h)
}

func MineBloco(tipo TipoBloco, dados any, chain *Chain) Bloco {
	chain.mu.RLock()
	var hashAnterior string
	index := len(chain.blocos)
	if index > 0 {
		hashAnterior = chain.blocos[index-1].Hash
	} else {
		hashAnterior = strings.Repeat("0", 64)
	}
	chain.mu.RUnlock()

	bloco := Bloco{
		Index:        index,
		Tipo:         tipo,
		Timestamp:    time.Now(),
		Dados:        dados,
		HashAnterior: hashAnterior,
		Nonce:        0,
	}

	prefixo := "00"
	for {
		bloco.Hash = HashBloco(bloco)
		if strings.HasPrefix(bloco.Hash, prefixo) {
			break
		}
		bloco.Nonce++
	}

	return bloco
}

func (c *Chain) ValidarChain() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i := 0; i < len(c.blocos); i++ {
		b := c.blocos[i]
		if b.Hash != HashBloco(b) {
			return false
		}
		if i > 0 {
			if b.HashAnterior != c.blocos[i-1].Hash {
				return false
			}
		}
		if !strings.HasPrefix(b.Hash, "00") {
			return false
		}
	}
	return true
}

func (c *Chain) SaldoAtual(zona string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	saldo := 0.0
	for _, b := range c.blocos {
		if b.Tipo == TipoCredito {
			var dados DadosCredito
			bytes, _ := json.Marshal(b.Dados)
			json.Unmarshal(bytes, &dados)
			if dados.Zona == zona {
				saldo += dados.Valor
			}
		}
	}
	return saldo
}

func TodosSaldos(c *Chain) map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	saldos := make(map[string]float64)
	for _, b := range c.blocos {
		if b.Tipo == TipoCredito {
			var dados DadosCredito
			bytes, _ := json.Marshal(b.Dados)
			json.Unmarshal(bytes, &dados)
			saldos[dados.Zona] += dados.Valor
		}
	}
	return saldos
}

func (c *Chain) AppendBloco(b Bloco) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.blocos) > 0 {
		ultimo := c.blocos[len(c.blocos)-1]
		if b.Index != ultimo.Index+1 {
			// Permite o mesmo índice se o hash for menor (conflito determinístico)
			if b.Index == ultimo.Index && b.Hash < ultimo.Hash {
				log.Printf("[BLOCKCHAIN] Conflito resolvido: substituindo bloco #%d por hash menor (%s < %s)\n",
					b.Index, b.Hash[:8], ultimo.Hash[:8])
				c.blocos[len(c.blocos)-1] = b
				return nil
			}
			return fmt.Errorf("índice inválido: esperado %d, recebido %d", ultimo.Index+1, b.Index)
		}
		if b.HashAnterior != ultimo.Hash {
			return fmt.Errorf("hash anterior não confere")
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
	c.blocos = append(c.blocos, b)
	return nil
}

func (c *Chain) GetChain() []Bloco {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copia := make([]Bloco, len(c.blocos))
	copy(copia, c.blocos)
	return copia
}

func (c *Chain) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.blocos)
}

func (c *Chain) ReplaceChain(novaChain []Bloco) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(novaChain) <= len(c.blocos) {
		return false
	}
	c.blocos = novaChain
	return true
}
