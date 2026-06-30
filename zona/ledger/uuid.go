package ledger

import (
	"crypto/rand"
	"fmt"
)

// novoUUID gera um UUID v4 (RFC 4122) usando crypto/rand.
// Evita dependência externa do pacote github.com/google/uuid.
func novoUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	// Versão 4
	b[6] = (b[6] & 0x0f) | 0x40
	// Variant bits
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
