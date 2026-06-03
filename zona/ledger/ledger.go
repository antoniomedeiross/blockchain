package ledger

import (
	"log"
	"time"
)

// Instancia é a chain global desta zona. Inicializada em IniciarLedger().
var Instancia *Chain

// Propagar é a função injetada pelo server.go para enviar um bloco novo
// a todos os peers via TCP. Evita dependência circular com o package repo.
var PropagaBloco func(bloco Bloco)

// IniciarLedger cria a chain com o bloco gênesis e guarda na variável global.
// Deve ser chamado uma vez no main, antes de aceitar conexões.
func IniciarLedger(zonaID string) {
	Instancia = NovaChain(zonaID)
}

// RegistrarPagamento debita créditos de uma empresa e adiciona o bloco à chain.
// Chamado antes de despachar o drone (em TentarAlocarDaFila).
// Retorna erro se a empresa não tiver saldo suficiente (double-spend bloqueado).
func RegistrarPagamento(empresaID string, droneID string, ocorrencia string, zona string) error {
	if Instancia == nil {
		log.Println("[LEDGER] ⚠ Ledger não inicializado")
		return nil // não bloqueia o sistema se ledger não tiver ativo
	}

	if !Instancia.TemSaldo(empresaID) {
		saldo := Instancia.SaldoEmpresa(empresaID)
		log.Printf("[LEDGER] ✗ Empresa %s sem créditos (saldo=%d, custo=%d)\n",
			empresaID, saldo, CustoPorRequisicao)
		return ErrSaldoInsuficiente
	}

	tx := Transacao{
		Tipo:       TxPagamento,
		EmpresaID:  empresaID,
		Creditos:   CustoPorRequisicao,
		DroneID:    droneID,
		Ocorrencia: ocorrencia,
		Zona:       zona,
		Timestamp:  time.Now(),
	}

	bloco, err := Instancia.AdicionarBloco(tx)
	if err != nil {
		return err
	}

	// Propaga o bloco minerado para os peers
	if PropagaBloco != nil {
		go PropagaBloco(bloco)
	}

	log.Printf("[LEDGER] 💳 Pagamento registrado — empresa=%s drone=%s saldo_restante=%d\n",
		empresaID, droneID, Instancia.SaldoEmpresa(empresaID))

	return nil
}

// RegistrarLaudo grava o resultado de uma missão como bloco imutável na chain.
// Chamado quando MISSAO_CONCLUIDA é recebido pelo handler.
// Não altera saldo — é puramente auditoria.
func RegistrarLaudo(empresaID string, droneID string, ocorrencia string, zona string) {
	if Instancia == nil {
		return
	}

	tx := Transacao{
		Tipo:       TxLaudo,
		EmpresaID:  empresaID,
		DroneID:    droneID,
		Ocorrencia: ocorrencia,
		Zona:       zona,
		Timestamp:  time.Now(),
	}

	bloco, err := Instancia.AdicionarBloco(tx)
	if err != nil {
		log.Printf("[LEDGER] ✗ Erro ao registrar laudo: %v\n", err)
		return
	}

	if PropagaBloco != nil {
		go PropagaBloco(bloco)
	}

	log.Printf("[LEDGER] 📋 Laudo registrado — empresa=%s drone=%s ocorrencia=%s hash=%s\n",
		empresaID, droneID, ocorrencia, bloco.Hash[:12])
}

// AceitarBlocoExterno recebe um bloco propagado por outro peer e o adiciona à chain local.
func AceitarBlocoExterno(bloco Bloco) {
	if Instancia == nil {
		return
	}
	if err := Instancia.AceitarBlocoExterno(bloco); err != nil {
		log.Printf("[LEDGER] ✗ Bloco externo rejeitado: %v\n", err)
	}
}

// ConsultarSaldo retorna o saldo atual de uma empresa.
func ConsultarSaldo(empresaID string) int {
	if Instancia == nil {
		return 0
	}
	return Instancia.SaldoEmpresa(empresaID)
}

// ErrSaldoInsuficiente é retornado quando a empresa não tem créditos.
var ErrSaldoInsuficiente = ledgerErro("saldo insuficiente para requisitar drone")

type ledgerErro string

func (e ledgerErro) Error() string { return string(e) }
