package ledger

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
)

// ChavePrivadaZona é a chave privada Ed25519 desta zona, carregada na inicialização.
// Usada para assinar todas as transações de pagamento emitidas por esta zona.
var ChavePrivadaZona ed25519.PrivateKey

// ChavePublicaZona é a chave pública correspondente (derivada automaticamente).
var ChavePublicaZona ed25519.PublicKey

// ChavesConhecidas mapeia zonaID -> chave pública hex, populado via SYNC e GENESIS.
// Qualquer peer pode verificar a assinatura de qualquer zona consultando este mapa.
var ChavesConhecidas = make(map[string]string)

// arquivoChave retorna o caminho do arquivo onde a chave privada desta zona fica persistida.
func arquivoChave(zonaID string) string {
	return fmt.Sprintf("chave_%s.hex", strings.ToLower(zonaID))
}

// IniciarIdentidade carrega (ou gera) o par de chaves Ed25519 desta zona.
// A chave privada é persistida em disco para sobreviver a reinicializações do container.
func IniciarIdentidade(zonaID string) {
	caminho := arquivoChave(zonaID)

	// Tenta carregar chave existente
	if dados, err := os.ReadFile(caminho); err == nil {
		privBytes, err := hex.DecodeString(strings.TrimSpace(string(dados)))
		if err == nil && len(privBytes) == ed25519.PrivateKeySize {
			ChavePrivadaZona = ed25519.PrivateKey(privBytes)
			ChavePublicaZona = ChavePrivadaZona.Public().(ed25519.PublicKey)
			log.Printf("[IDENTIDADE] ✔ Chave Ed25519 carregada para zona %s — pubkey: %s\n",
				zonaID, hex.EncodeToString(ChavePublicaZona)[:16]+"...")
			return
		}
	}

	// Gera novo par de chaves
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatalf("[IDENTIDADE] ✗ Falha ao gerar chaves Ed25519: %v", err)
	}

	ChavePrivadaZona = privKey
	ChavePublicaZona = pubKey

	// Persiste a chave privada em hex
	if err := os.WriteFile(caminho, []byte(hex.EncodeToString(privKey)), 0600); err != nil {
		log.Printf("[IDENTIDADE] ⚠ Não foi possível salvar a chave privada: %v\n", err)
	}

	log.Printf("[IDENTIDADE] ✔ Novo par Ed25519 gerado para zona %s — pubkey: %s\n",
		zonaID, hex.EncodeToString(pubKey)[:16]+"...")
}

// RegistrarChavePublica associa uma chave pública hex a uma zona no mapa local.
// Chamado durante o SYNC ao receber a chave pública de um peer.
func RegistrarChavePublica(zonaID, pubKeyHex string) {
	if zonaID == "" || pubKeyHex == "" {
		return
	}
	ChavesConhecidas[zonaID] = pubKeyHex
	log.Printf("[IDENTIDADE] 📬 Chave pública de %s registrada: %s...\n", zonaID, pubKeyHex[:16])
}

// ChavePublicaHex retorna a chave pública desta zona em formato hexadecimal.
func ChavePublicaHex() string {
	return hex.EncodeToString(ChavePublicaZona)
}
