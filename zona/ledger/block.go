package ledger

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// TipoTransacao classifica o que está sendo registrado no bloco.
type TipoTransacao string

const (
	TxPagamento TipoTransacao = "PAGAMENTO"  // zona gastou créditos para requisitar um drone
	TxLaudo     TipoTransacao = "LAUDO"       // drone concluiu missão — laudo de operação
	TxGenesis   TipoTransacao = "GENESIS"     // bloco inicial com saldo de cada zona
)

// Transacao é o payload dentro de cada bloco.
// Um bloco tem exatamente uma transação para simplificar o encadeamento.
type Transacao struct {
	Tipo      TipoTransacao `json:"tipo"`
	ZonaID    string        `json:"zona_id"`             // quem pagou / quem foi atendido
	Creditos  int           `json:"creditos"`            // quantos créditos foram gastos (negativo = débito)
	DroneID   string        `json:"drone_id,omitempty"`  // drone envolvido
	Ocorrencia string       `json:"ocorrencia,omitempty"` // descrição do evento (para laudos)
	Zona      string        `json:"zona,omitempty"`      // zona onde ocorreu
	Timestamp time.Time     `json:"timestamp"`
}

// Bloco é a unidade da chain. Cada bloco aponta para o hash do anterior,
// formando a cadeia imutável. O campo Nonce é o resultado do PoW.
type Bloco struct {
	Index     int        `json:"index"`
	Timestamp time.Time  `json:"timestamp"`
	Tx        Transacao  `json:"transacao"`
	HashAnterior string  `json:"hash_anterior"`
	Hash      string     `json:"hash"`
	Nonce     int        `json:"nonce"`     // encontrado pelo PoW
	Minerador string     `json:"minerador"` // zona que minerou este bloco
}

// dificuldadePoW define quantos zeros o hash precisa ter no início.
// 2 zeros = ~256 tentativas em média, suficiente para demo sem travar o sistema.
const dificuldadePoW = 2

// calcularHash gera o SHA256 do bloco (sem o campo Hash em si).
func calcularHash(b Bloco) string {
	// serializa tudo menos o campo Hash para não criar dependência circular
	// Usa UnixNano() em vez de String() para evitar perda de precisão ou do relógio monotônico via JSON
	registro := fmt.Sprintf("%d|%d|%s|%s|%d|%s|%s|%s|%d|%s|%d|%s",
		b.Index,
		b.Timestamp.UnixNano(),
		b.Tx.Tipo,
		b.Tx.ZonaID,
		b.Tx.Creditos,
		b.Tx.DroneID,
		b.Tx.Ocorrencia,
		b.Tx.Zona,
		b.Tx.Timestamp.UnixNano(),
		b.HashAnterior,
		b.Nonce,
		b.Minerador,
	)
	h := sha256.Sum256([]byte(registro))
	return fmt.Sprintf("%x", h)
}

// Minerar executa o Proof-of-Work: incrementa Nonce até o hash começar com
// `dificuldadePoW` zeros. Retorna o bloco com Hash e Nonce preenchidos.
func Minerar(b Bloco) Bloco {
	prefixo := strings.Repeat("0", dificuldadePoW)
	for {
		b.Hash = calcularHash(b)
		if strings.HasPrefix(b.Hash, prefixo) {
			break
		}
		b.Nonce++
	}
	return b
}

// NovoBloco cria um bloco ainda sem hash (precisa chamar Minerar depois).
func NovoBloco(index int, hashAnterior string, tx Transacao, minerador string) Bloco {
	return Bloco{
		Index:        index,
		Timestamp:    time.Now(),
		Tx:           tx,
		HashAnterior: hashAnterior,
		Hash:         "",
		Nonce:        0,
		Minerador:    minerador,
	}
}

// Validar verifica se o hash do bloco é consistente com seu conteúdo
// e atende a dificuldade do PoW.
func (b *Bloco) Validar() bool {
	hashEsperado := calcularHash(*b)
	prefixo := strings.Repeat("0", dificuldadePoW)
	return b.Hash == hashEsperado && strings.HasPrefix(b.Hash, prefixo)
}

// String retorna representação legível do bloco para logs.
func (b *Bloco) String() string {
	dados, _ := json.MarshalIndent(b, "  ", "  ")
	return string(dados)
}
