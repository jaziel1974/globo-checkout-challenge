# Validação da iteração 3 — Simulação e resultados

Data: 22/11/2026.

## Resultado

Etapa aprovada. O gate com Redis real passou com 59 testes: BFF 39 e worker 20, sem falhas, erros ou skips (`BUILD SUCCESS`, 1m08s). O teste unitário focado executado antes do gate passou com 11 testes, também sem falhas, erros ou skips.

## Comandos e evidências

- `./mvnw.cmd -pl checkout-worker -am '-Dtest=PaymentSimulatorTest,PaymentProcessorTest,RedisCartSnapshotRepositoryTest,V1CartPayloadReaderTest,PaymentCommandTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` — 11 testes, `BUILD SUCCESS`, sem skips.
- `./mvnw.cmd -pl checkout-worker -am test` — BFF 39 + worker 20 = 59 testes; zero falhas, zero erros, zero skips; Redis real via Docker; `BUILD SUCCESS` (1m08s), conforme resultado do gate executado pelo agente principal.

Os testes exercitam cartão aprovado e recusado, crédito/débito, Pix, total decimal derivado do snapshot v1, validação do snapshot e desserialização do `payload` Java da v1. O processador persiste o resultado sob posse válida. O teste do adaptador de carrinho garante leitura do estado atual e ausência de escrita no carrinho v1; a conclusão grava aprovação somente nas chaves próprias v2.

## Limites

A regra de recusa pode ser seguida de nova tentativa no domínio, mas o fluxo com Kafka real ainda não está conectado. Reentrega, offset, recuperação em processo independente, fila de falhas e comportamento HTTP serão validados nas iterações 5 a 7. Os testes de expiração/posse do Redis não demonstram renovação periódica real pelo consumidor; isso pertence à iteração 5.
