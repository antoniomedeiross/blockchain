package ledger

import (
	"fmt"
	"log"
	"time"
)

// ProporBlocoFn é injetado pelo server.go para executar o consenso 2/3 antes de
// persistir um bloco. Quando nil (ex: testes), o bloco é aceito localmente sem votação.
var ProporBlocoFn func(bloco Bloco) (Bloco, error)

// Instancia é a chain global desta zona. Inicializada em IniciarLedger().
var Instancia *Chain

// PropagaBloco é a função injetada pelo server.go para enviar um bloco novo
// a todos os peers via TCP. Evita dependência circular com o package repo.
var PropagaBloco func(bloco Bloco)

// IniciarLedger cria a chain carregando dados do arquivo ou gerando novo gênesis.
func IniciarLedger(zonaID string) {
	// Inicializa identidade criptográfica da zona (chaves Ed25519)
	IniciarIdentidade(zonaID)

	blocos, err := CarregarLedger()
	if err != nil {
		log.Printf("[LEDGER] ⚠ Falha ao carregar arquivo ledger: %v. Iniciando nova.\n", err)
	}
	Instancia = NovaChain(zonaID, blocos)
}

// RegistrarPagamento debita créditos de uma zona via consenso 2/3 e adiciona o bloco à chain.
// A transação é assinada com a chave Ed25519 desta zona antes do consenso.
func RegistrarPagamento(zonaID string, droneID string, ocorrencia string, zona string) error {
	if Instancia == nil {
		log.Println("[LEDGER] ⚠ Ledger não inicializado")
		return nil
	}

	if !Instancia.TemSaldo(zonaID) {
		saldo := Instancia.SaldoZona(zonaID)
		log.Printf("[LEDGER] ✗ Zona %s sem créditos (saldo=%d, custo=%d)\n",
			zonaID, saldo, CustoPorRequisicao)
		return ErrSaldoInsuficiente
	}

	tx := Transacao{
		Tipo:       TxPagamento,
		ZonaID:     zonaID,
		Creditos:   CustoPorRequisicao,
		DroneID:    droneID,
		Ocorrencia: ocorrencia,
		Zona:       zona,
		Timestamp:  time.Now(),
		TxID:       novoUUID(), // UUID único — previne duplo gasto
	}

	// Assina a transação com a chave privada desta zona
	if ChavePrivadaZona != nil {
		AssinarTransacao(&tx, ChavePrivadaZona)
		log.Printf("[LEDGER] 🔑 Transação %s assinada com chave Ed25519\n", tx.TxID[:8])
	} else {
		log.Printf("[LEDGER] ⚠ Chave privada não inicializada — transação sem assinatura\n")
	}

	bloco, err := Instancia.AdicionarBloco(tx)
	if err != nil {
		return err
	}

	// Consenso 2/3: propaga para peers votarem antes de confirmar
	if ProporBlocoFn != nil {
		bloco, err = ProporBlocoFn(bloco)
		if err != nil {
			Instancia.RemoverUltimoBloco()
			return err
		}
	}

	SalvarBloco(bloco)

	if PropagaBloco != nil {
		go PropagaBloco(bloco)
	}

	log.Printf("[LEDGER] 💳 Pagamento registrado — zona=%s drone=%s txid=%s saldo_restante=%d\n",
		zonaID, droneID, tx.TxID[:8], Instancia.SaldoZona(zonaID))

	return nil
}

// RegistrarLaudo grava o resultado de uma missão como bloco imutável na chain via consenso.
// Não altera saldo — é puramente auditoria.
func RegistrarLaudo(zonaID string, droneID string, ocorrencia string, zona string) {
	if Instancia == nil {
		return
	}

	tx := Transacao{
		Tipo:       TxLaudo,
		ZonaID:     zonaID,
		DroneID:    droneID,
		Ocorrencia: ocorrencia,
		Zona:       zona,
		Timestamp:  time.Now(),
		TxID:       novoUUID(),
	}

	bloco, err := Instancia.AdicionarBloco(tx)
	if err != nil {
		log.Printf("[LEDGER] ✗ Erro ao minerar laudo: %v\n", err)
		return
	}

	if ProporBlocoFn != nil {
		bloco, err = ProporBlocoFn(bloco)
		if err != nil {
			log.Printf("[LEDGER] ✗ Consenso do laudo falhou: %v\n", err)
			Instancia.RemoverUltimoBloco()
			return
		}
	}

	SalvarBloco(bloco)

	if PropagaBloco != nil {
		go PropagaBloco(bloco)
	}

	log.Printf("[LEDGER] 📋 Laudo registrado — zona=%s drone=%s ocorrencia=%s hash=%s\n",
		zonaID, droneID, ocorrencia, bloco.Hash[:12])
}

// AceitarBlocoExterno recebe um bloco propagado por outro peer e o adiciona à chain local.
func AceitarBlocoExterno(bloco Bloco) {
	if Instancia == nil {
		return
	}
	if err := Instancia.AceitarBlocoExterno(bloco); err != nil {
		log.Printf("[LEDGER] ✗ Bloco externo rejeitado: %v\n", err)
		return
	}
	SalvarBloco(bloco)
}

// ConsultarSaldo retorna o saldo atual de uma zona.
func ConsultarSaldo(zonaID string) int {
	if Instancia == nil {
		return 0
	}
	return Instancia.SaldoZona(zonaID)
}

// ErrSaldoInsuficiente é retornado quando a zona não tem créditos.
var ErrSaldoInsuficiente = ledgerErro("saldo insuficiente para requisitar drone")

type ledgerErro string

func (e ledgerErro) Error() string { return string(e) }

// ChavePublicaDaZona retorna a chave pública hex desta zona (para anunciar aos peers no SYNC).
func ChavePublicaDaZona() string {
	return ChavePublicaHex()
}

// RegistrarChavePeer registra a chave pública de um peer para validação futura.
func RegistrarChavePeer(zonaID, pubKeyHex string) {
	RegistrarChavePublica(zonaID, pubKeyHex)
}

// ValidarChainCompleta valida toda a chain local (hash + assinaturas).
func ValidarChainCompleta() bool {
	if Instancia == nil {
		return false
	}
	return Instancia.ValidarChain()
}

// InfoSeguranca retorna informações sobre o estado de segurança do ledger desta zona.
func InfoSeguranca() map[string]interface{} {
	info := map[string]interface{}{
		"pubkey":         ChavePublicaHex(),
		"chain_valida":   ValidarChainCompleta(),
		"peers_conhecidos": len(ChavesConhecidas),
	}
	if Instancia != nil {
		info["total_blocos"] = Instancia.Tamanho()
		info["total_txids"]  = fmt.Sprintf("%d TxIDs indexados", Instancia.TotalTxIDs())
	}
	return info
}
