package ledger

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// TipoTransacao classifica o que está sendo registrado no bloco.
type TipoTransacao string

const (
	TxPagamento TipoTransacao = "PAGAMENTO" // zona gastou créditos para requisitar um drone
	TxLaudo     TipoTransacao = "LAUDO"     // drone concluiu missão — laudo de operação
	TxGenesis   TipoTransacao = "GENESIS"   // bloco inicial com saldo de cada zona
)

// Transacao é o payload dentro de cada bloco.
// TxID garante idempotência: a rede rejeita qualquer transação cujo TxID já exista na chain.
// Assinatura (Ed25519) prova que a zona dona dos créditos autorizou o débito.
type Transacao struct {
	Tipo       TipoTransacao `json:"tipo"`
	ZonaID     string        `json:"zona_id"`              // quem pagou / quem foi atendido
	Creditos   int           `json:"creditos"`             // quantos créditos foram gastos
	DroneID    string        `json:"drone_id,omitempty"`   // drone envolvido
	Ocorrencia string        `json:"ocorrencia,omitempty"` // descrição do evento (para laudos)
	Zona       string        `json:"zona,omitempty"`       // zona onde ocorreu
	Timestamp  time.Time     `json:"timestamp"`

	// ── Campos de segurança adicionados ──────────────────────────────────────
	TxID      string `json:"tx_id"`                // UUID único — previne duplo gasto / replay
	Assinatura string `json:"assinatura,omitempty"` // hex(Ed25519) sobre TxID+ZonaID+Creditos+Timestamp
	ChavePublica string `json:"chave_publica,omitempty"` // hex(Ed25519 pubkey) da zona emissora
}

// Bloco é a unidade da chain. Cada bloco aponta para o hash do anterior,
// formando a cadeia imutável. O campo Nonce é o resultado do PoW.
type Bloco struct {
	Index        int       `json:"index"`
	Timestamp    time.Time `json:"timestamp"`
	Tx           Transacao `json:"transacao"`
	HashAnterior string    `json:"hash_anterior"`
	Hash         string    `json:"hash"`
	Nonce        int       `json:"nonce"`     // encontrado pelo PoW
	Minerador    string    `json:"minerador"` // zona que minerou este bloco
}

// dificuldadePoW define quantos zeros o hash precisa ter no início.
// 2 zeros = ~256 tentativas em média, suficiente para demo sem travar o sistema.
const dificuldadePoW = 2

// calcularHash gera o SHA256 do bloco (sem o campo Hash em si).
// TxID e Assinatura fazem parte do hash — adulterá-los quebra a cadeia.
func calcularHash(b Bloco) string {
	registro := fmt.Sprintf("%d|%d|%s|%s|%d|%s|%s|%s|%d|%s|%s|%s|%d|%s",
		b.Index,
		b.Timestamp.UnixNano(),
		b.Tx.Tipo,
		b.Tx.ZonaID,
		b.Tx.Creditos,
		b.Tx.DroneID,
		b.Tx.Ocorrencia,
		b.Tx.Zona,
		b.Tx.Timestamp.UnixNano(),
		b.Tx.TxID,       // <- novo campo na hash
		b.Tx.Assinatura, // <- assinatura entra no hash
		b.HashAnterior,
		b.Nonce,
		b.Minerador,
	)
	h := sha256.Sum256([]byte(registro))
	return fmt.Sprintf("%x", h)
}

// mensagemParaAssinar retorna o payload canônico que a zona assina com sua chave privada.
// Qualquer peer pode verificar usando a ChavePublica embutida na transação.
func mensagemParaAssinar(tx Transacao) []byte {
	payload := fmt.Sprintf("%s|%s|%d|%d",
		tx.TxID,
		tx.ZonaID,
		tx.Creditos,
		tx.Timestamp.UnixNano(),
	)
	return []byte(payload)
}

// AssinarTransacao assina a transação com a chave privada da zona e preenche
// os campos Assinatura e ChavePublica.
func AssinarTransacao(tx *Transacao, privKey ed25519.PrivateKey) {
	msg := mensagemParaAssinar(*tx)
	sig := ed25519.Sign(privKey, msg)
	tx.Assinatura = hex.EncodeToString(sig)
	tx.ChavePublica = hex.EncodeToString(privKey.Public().(ed25519.PublicKey))
}

// VerificarAssinatura valida a assinatura Ed25519 de uma transação.
// Retorna false se a chave pública ou a assinatura forem inválidas/ausentes.
func VerificarAssinatura(tx Transacao) bool {
	if tx.Assinatura == "" || tx.ChavePublica == "" {
		return false
	}
	pubBytes, err := hex.DecodeString(tx.ChavePublica)
	if err != nil || len(pubBytes) != ed25519.PublicKeySize {
		return false
	}
	sigBytes, err := hex.DecodeString(tx.Assinatura)
	if err != nil {
		return false
	}
	msg := mensagemParaAssinar(tx)
	return ed25519.Verify(ed25519.PublicKey(pubBytes), msg, sigBytes)
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
