package ledger

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// saldoInicialPorZona define os créditos que cada zona começa com no bloco gênesis.
var saldoInicialPorZona = map[string]int{
	"NORTE": 200,
	"SUL":   200,
	"LESTE": 200,
}

// Ordem determinística para gerar o gênesis (evita divergências de hash entre os nodes)
var ordemZonasGenesis = []string{
	"NORTE",
	"SUL",
	"LESTE",
}

// CustoPorRequisicao é quantos créditos uma requisição de drone consome.
const CustoPorRequisicao = 10

// Chain é a blockchain em si: uma slice de blocos protegida por mutex.
type Chain struct {
	Blocos []Bloco
	mu     sync.RWMutex

	// ZonaID identifica esta zona para assinar blocos minerados aqui.
	ZonaID string

	// txIDs é o conjunto de TxIDs já registrados — previne duplo gasto e replay.
	txIDs map[string]bool
}

// NovaChain cria uma chain. Se blocosPersistidos não for nulo, usa eles.
// Caso contrário, adiciona o bloco gênesis com os saldos iniciais.
func NovaChain(zonaID string, blocosPersistidos []Bloco) *Chain {
	c := &Chain{ZonaID: zonaID, txIDs: make(map[string]bool)}

	if len(blocosPersistidos) > 0 {
		// Reconstrói o índice de TxIDs a partir dos blocos persistidos
		for _, b := range blocosPersistidos {
			if b.Tx.TxID != "" {
				c.txIDs[b.Tx.TxID] = true
			}
		}
		c.Blocos = blocosPersistidos
		log.Printf("[LEDGER] ✔ Chain carregada do arquivo com %d blocos\n", len(c.Blocos))
		return c
	}

	c.Blocos = []Bloco{}

	// Tempo base determinístico para os blocos gênesis
	tempoGenesis := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, zona := range ordemZonasGenesis {
		saldo := saldoInicialPorZona[zona]
		tx := Transacao{
			Tipo:      TxGenesis,
			ZonaID:    zona,
			Creditos:  saldo,
			Timestamp: tempoGenesis,
			TxID:      "genesis-" + zona, // TxID fixo e único para o gênesis
		}
		var hashAnterior string
		if len(c.Blocos) == 0 {
			hashAnterior = "0000000000000000"
		} else {
			hashAnterior = c.Blocos[len(c.Blocos)-1].Hash
		}

		bloco := Bloco{
			Index:        len(c.Blocos),
			Timestamp:    tempoGenesis,
			Tx:           tx,
			HashAnterior: hashAnterior,
			Hash:         "",
			Nonce:        0,
			Minerador:    "SISTEMA",
		}

		bloco = Minerar(bloco)
		c.Blocos = append(c.Blocos, bloco)
		c.txIDs[tx.TxID] = true

		SalvarBloco(bloco)

		log.Printf("[LEDGER] ⛏  Gênesis determinístico — zona=%s saldo=%d hash=%s\n",
			zona, saldo, bloco.Hash[:12])
	}

	log.Printf("[LEDGER] ✔ Chain inicializada com %d blocos gênesis\n", len(c.Blocos))
	return c
}

// txIDJaExiste verifica se um TxID já foi registrado (sem lock — use com mu já adquirido).
func (c *Chain) txIDJaExiste(txID string) bool {
	return c.txIDs[txID]
}

// AdicionarBloco minera e appenda um bloco à chain local.
// Retorna o bloco minerado (para propagar aos peers).
func (c *Chain) AdicionarBloco(tx Transacao) (Bloco, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Blocos) == 0 {
		return Bloco{}, fmt.Errorf("chain sem gênesis")
	}

	// Prevenção de duplo gasto / replay: TxID já registrado = rejeita
	if tx.TxID != "" && c.txIDJaExiste(tx.TxID) {
		return Bloco{}, fmt.Errorf("duplo gasto detectado: TxID %s já registrado na chain", tx.TxID)
	}

	hashAnterior := c.Blocos[len(c.Blocos)-1].Hash
	bloco := NovoBloco(len(c.Blocos), hashAnterior, tx, c.ZonaID)
	bloco = Minerar(bloco)

	c.Blocos = append(c.Blocos, bloco)
	if tx.TxID != "" {
		c.txIDs[tx.TxID] = true
	}

	log.Printf("[LEDGER] ⛏  Bloco #%d minerado — tipo=%s zona=%s nonce=%d hash=%s\n",
		bloco.Index, tx.Tipo, tx.ZonaID, bloco.Nonce, bloco.Hash[:12])

	return bloco, nil
}

// AceitarBlocoExterno adiciona um bloco recebido de outro peer à chain local,
// após validar hash, encadeamento, saldo, TxID (duplo gasto) e assinatura Ed25519.
func (c *Chain) AceitarBlocoExterno(bloco Bloco) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Já temos este bloco?
	if bloco.Index < len(c.Blocos) {
		blocoExistente := c.Blocos[bloco.Index]
		if blocoExistente.Hash == bloco.Hash {
			return nil // já temos — ignora silenciosamente
		}
		return fmt.Errorf("conflito no bloco #%d: hash recebido %s != local %s",
			bloco.Index, bloco.Hash[:12], blocoExistente.Hash[:12])
	}

	// 2. Valida o hash e o PoW
	if !bloco.Validar() {
		return fmt.Errorf("bloco #%d inválido: hash não bate ou PoW insuficiente", bloco.Index)
	}

	// 3. Verifica encadeamento
	if len(c.Blocos) > 0 {
		topo := c.Blocos[len(c.Blocos)-1]
		if bloco.Index != topo.Index+1 {
			return fmt.Errorf("bloco #%d fora de ordem: topo atual é #%d", bloco.Index, topo.Index)
		}
		if bloco.HashAnterior != topo.Hash {
			return fmt.Errorf("bloco #%d fora de ordem: esperado hash_anterior=%s, recebido=%s",
				bloco.Index, topo.Hash[:12], bloco.HashAnterior[:12])
		}
	}

	tx := bloco.Tx

	// 4. Prevenção de duplo gasto: TxID já na chain?
	if tx.TxID != "" && c.txIDJaExiste(tx.TxID) {
		return fmt.Errorf("duplo gasto detectado no bloco externo: TxID %s já registrado", tx.TxID)
	}

	// 5. Validação de regras de negócio por tipo de transação
	switch tx.Tipo {
	case TxGenesis:
		return fmt.Errorf("tentativa de injetar bloco GÊNESIS externo negada")

	case TxPagamento:
		// 5a. Verifica assinatura Ed25519 — prova que a zona autorizou o débito
		if tx.Assinatura == "" || tx.ChavePublica == "" {
			return fmt.Errorf("bloco #%d rejeitado: transação de pagamento sem assinatura digital", bloco.Index)
		}
		if !VerificarAssinatura(tx) {
			return fmt.Errorf("bloco #%d rejeitado: assinatura Ed25519 INVÁLIDA para zona %s", bloco.Index, tx.ZonaID)
		}

		// 5b. Verifica saldo suficiente (contra a chain atual sem este bloco)
		saldo := c.calcularSaldoSemLock(tx.ZonaID)
		if saldo < tx.Creditos {
			return fmt.Errorf("zona %s tentou gastar %d mas tinha apenas %d", tx.ZonaID, tx.Creditos, saldo)
		}
	}

	// 6. Aceita
	c.Blocos = append(c.Blocos, bloco)
	if tx.TxID != "" {
		c.txIDs[tx.TxID] = true
	}

	log.Printf("[LEDGER] ✔ Bloco externo #%d aceito — tipo=%s zona=%s minerador=%s\n",
		bloco.Index, bloco.Tx.Tipo, bloco.Tx.ZonaID, bloco.Minerador)
	return nil
}

// calcularSaldoSemLock é a lógica de saldo sem o mutex (usada internamente)
func (c *Chain) calcularSaldoSemLock(zonaID string) int {
	saldo := 0
	for _, bloco := range c.Blocos {
		tx := bloco.Tx
		if tx.ZonaID != zonaID {
			continue
		}
		switch tx.Tipo {
		case TxGenesis:
			saldo += tx.Creditos
		case TxPagamento:
			saldo -= tx.Creditos
		}
	}
	return saldo
}

// SaldoZona percorre toda a chain e calcula o saldo atual de uma zona.
func (c *Chain) SaldoZona(zonaID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.calcularSaldoSemLock(zonaID)
}

// TemSaldo retorna true se a zona tiver créditos suficientes para uma requisição.
func (c *Chain) TemSaldo(zonaID string) bool {
	return c.SaldoZona(zonaID) >= CustoPorRequisicao
}

// ValidarChain percorre a chain inteira verificando cada bloco e as assinaturas.
func (c *Chain) ValidarChain() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := 1; i < len(c.Blocos); i++ {
		atual := c.Blocos[i]
		anterior := c.Blocos[i-1]

		if !atual.Validar() {
			log.Printf("[LEDGER] ✗ Bloco #%d com hash inválido!\n", i)
			return false
		}
		if atual.HashAnterior != anterior.Hash {
			log.Printf("[LEDGER] ✗ Encadeamento quebrado entre bloco #%d e #%d!\n", i-1, i)
			return false
		}
		// Valida assinatura em blocos de pagamento
		if atual.Tx.Tipo == TxPagamento && !VerificarAssinatura(atual.Tx) {
			log.Printf("[LEDGER] ✗ Bloco #%d com assinatura Ed25519 inválida!\n", i)
			return false
		}
	}
	return true
}

// Snapshot retorna uma cópia da chain para serialização.
func (c *Chain) Snapshot() []Bloco {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copia := make([]Bloco, len(c.Blocos))
	copy(copia, c.Blocos)
	return copia
}

// Tamanho retorna quantos blocos a chain tem.
func (c *Chain) Tamanho() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.Blocos)
}

// TotalTxIDs retorna a contagem de TxIDs indexados (para auditoria).
func (c *Chain) TotalTxIDs() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.txIDs)
}

// RemoverUltimoBloco faz rollback do último bloco — usado quando o consenso
// falha após AdicionarBloco já ter appendado localmente.
func (c *Chain) RemoverUltimoBloco() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Blocos) > 0 {
		removido := c.Blocos[len(c.Blocos)-1]
		// Remove o TxID do índice junto com o bloco
		if removido.Tx.TxID != "" {
			delete(c.txIDs, removido.Tx.TxID)
		}
		c.Blocos = c.Blocos[:len(c.Blocos)-1]
		log.Printf("[LEDGER] ↩ Rollback: bloco #%d removido (consenso falhou)\n", removido.Index)
	}
}
