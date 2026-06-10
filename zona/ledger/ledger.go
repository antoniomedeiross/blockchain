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

// IniciarLedger cria a chain carregando dados do arquivo ou gerando novo gênesis.
func IniciarLedger(zonaID string) {
	blocos, err := CarregarLedger()
	if err != nil {
		log.Printf("[LEDGER] ⚠ Falha ao carregar arquivo ledger: %v. Iniciando nova.\n", err)
	}
	Instancia = NovaChain(zonaID, blocos)
}

// RegistrarPagamento debita créditos de uma zona e adiciona o bloco à chain.
// Chamado antes de despachar o drone (em TentarAlocarDaFila).
// Retorna erro se a zona não tiver saldo suficiente (double-spend bloqueado).
func RegistrarPagamento(zonaID string, droneID string, ocorrencia string, zona string) error {
	if Instancia == nil {
		log.Println("[LEDGER] ⚠ Ledger não inicializado")
		return nil // não bloqueia o sistema se ledger não tiver ativo
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
	}

	bloco, err := Instancia.AdicionarBloco(tx)
	if err != nil {
		return err
	}

	// Salva no arquivo local
	SalvarBloco(bloco)

	// Propaga o bloco minerado para los peers
	if PropagaBloco != nil {
		go PropagaBloco(bloco)
	}

	log.Printf("[LEDGER] 💳 Pagamento registrado — zona=%s drone=%s saldo_restante=%d\n",
		zonaID, droneID, Instancia.SaldoZona(zonaID))

	return nil
}

// RegistrarLaudo grava o resultado de uma missão como bloco imutável na chain.
// Chamado quando MISSAO_CONCLUIDA é recebido pelo handler.
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
	}

	bloco, err := Instancia.AdicionarBloco(tx)
	if err != nil {
		log.Printf("[LEDGER] ✗ Erro ao registrar laudo: %v\n", err)
		return
	}

	// Salva no arquivo local
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
	// Salva o bloco externo aceito no arquivo local
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
