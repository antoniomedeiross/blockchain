package ledger

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// saldoInicialPorEmpresa define os créditos que cada empresa começa com no bloco gênesis.
// Empresas não listadas aqui não têm saldo e não podem requisitar drones.
var saldoInicialPorEmpresa = map[string]int{
	"empresa-alpha":   100,
	"empresa-beta":    80,
	"empresa-gamma":   60,
	"empresa-delta":   40,
}

// Ordem determinística para gerar o gênesis (evita divergências de hash entre os nodes)
var ordemEmpresasGenesis = []string{
	"empresa-alpha",
	"empresa-beta",
	"empresa-gamma",
	"empresa-delta",
}

// CustoPorRequisicao é quantos créditos uma requisição de drone consome.
const CustoPorRequisicao = 10

// Chain é a blockchain em si: uma slice de blocos protegida por mutex.
type Chain struct {
	Blocos []Bloco
	mu     sync.RWMutex

	// ZonaID identifica esta zona para assinar blocos minerados aqui.
	ZonaID string
}

// NovaChain cria uma chain vazia e adiciona o bloco gênesis com os saldos iniciais.
func NovaChain(zonaID string) *Chain {
	c := &Chain{ZonaID: zonaID}
	c.Blocos = []Bloco{}

	// Tempo base determinístico para os blocos gênesis (garante que todos os peers gerem o mesmo hash de origem)
	tempoGenesis := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Bloco gênesis: uma transação especial que "emite" os créditos de cada empresa.
	// Não representa gasto — é o ponto de partida imutável da ledger.
	for _, empresa := range ordemEmpresasGenesis {
		saldo := saldoInicialPorEmpresa[empresa]
		tx := Transacao{
			Tipo:      TxGenesis,
			EmpresaID: empresa,
			Creditos:  saldo, // positivo = emissão inicial
			Timestamp: tempoGenesis,
		}
		var hashAnterior string
		if len(c.Blocos) == 0 {
			hashAnterior = "0000000000000000" // raiz da cadeia
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
			// Hardcoded minerador para gênesis ser idêntico em todas as máquinas
			Minerador:    "SISTEMA", 
		}

		bloco = Minerar(bloco)
		c.Blocos = append(c.Blocos, bloco)
		log.Printf("[LEDGER] ⛏  Gênesis determinístico — empresa=%s saldo=%d hash=%s\n",
			empresa, saldo, bloco.Hash[:12])
	}

	log.Printf("[LEDGER] ✔ Chain inicializada com %d blocos gênesis\n", len(c.Blocos))
	return c
}

// AdicionarBloco minera e appenda um bloco à chain local.
// Retorna o bloco minerado (para propagar aos peers).
func (c *Chain) AdicionarBloco(tx Transacao) (Bloco, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Blocos) == 0 {
		return Bloco{}, fmt.Errorf("chain sem gênesis")
	}

	hashAnterior := c.Blocos[len(c.Blocos)-1].Hash
	bloco := NovoBloco(len(c.Blocos), hashAnterior, tx, c.ZonaID)
	bloco = Minerar(bloco)

	c.Blocos = append(c.Blocos, bloco)
	log.Printf("[LEDGER] ⛏  Bloco #%d minerado — tipo=%s empresa=%s nonce=%d hash=%s\n",
		bloco.Index, tx.Tipo, tx.EmpresaID, bloco.Nonce, bloco.Hash[:12])

	return bloco, nil
}

// AceitarBlocoExterno adiciona um bloco recebido de outro peer à chain local,
// após validar o hash e o encadeamento. Evita minerar de novo — já foi minerado
// pelo peer que propagou.
func (c *Chain) AceitarBlocoExterno(bloco Bloco) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Valida o hash e o PoW do bloco recebido
	if !bloco.Validar() {
		return fmt.Errorf("bloco #%d inválido: hash não bate ou PoW insuficiente", bloco.Index)
	}

	// 2. Verifica encadeamento: HashAnterior deve ser o hash do topo atual
	if len(c.Blocos) > 0 {
		topo := c.Blocos[len(c.Blocos)-1]
		if bloco.HashAnterior != topo.Hash {
			return fmt.Errorf("bloco #%d fora de ordem: esperado hash_anterior=%s, recebido=%s",
				bloco.Index, topo.Hash[:12], bloco.HashAnterior[:12])
		}
	}

	// 3. Aceita
	c.Blocos = append(c.Blocos, bloco)
	log.Printf("[LEDGER] ✔ Bloco externo #%d aceito — tipo=%s empresa=%s minerador=%s\n",
		bloco.Index, bloco.Tx.Tipo, bloco.Tx.EmpresaID, bloco.Minerador)
	return nil
}

// SaldoEmpresa percorre toda a chain e calcula o saldo atual de uma empresa.
// Gênesis soma, PAGAMENTO subtrai, LAUDO não altera saldo (só registra).
func (c *Chain) SaldoEmpresa(empresaID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	saldo := 0
	for _, bloco := range c.Blocos {
		tx := bloco.Tx
		if tx.EmpresaID != empresaID {
			continue
		}
		switch tx.Tipo {
		case TxGenesis:
			saldo += tx.Creditos // emissão inicial
		case TxPagamento:
			saldo -= tx.Creditos // débito (Creditos armazena o valor positivo do gasto)
		// TxLaudo não muda saldo — é só registro de auditoria
		}
	}
	return saldo
}

// TemSaldo retorna true se a empresa tiver créditos suficientes para uma requisição.
func (c *Chain) TemSaldo(empresaID string) bool {
	return c.SaldoEmpresa(empresaID) >= CustoPorRequisicao
}

// ValidarChain percorre a chain inteira verificando cada bloco.
// Útil para auditoria completa.
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
	}
	return true
}

// Snapshot retorna uma cópia da chain para serialização (ex: HTTP ou propagação).
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
