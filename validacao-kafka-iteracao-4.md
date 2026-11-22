# Validação da iteração 4 — Publicação Kafka

Data: 22/11/2026.

## Resultado

Etapa aprovada após corrigir uma falha de leitura de prazo. O script `scripts/verify-kafka-stage.ps1` verificou Docker 27.1.1 e executou Kafka 4.2.2 e Redis reais. Gate final: 16 testes, zero falhas, zero erros, zero skips; `BUILD SUCCESS` em 42.236s. O script confirmou que os relatórios eram atuais e sem skips.

## Falha anterior e correção

A primeira execução final do gate teve 1 falha entre 16 testes. `applicationCrashAfterReservationConsumesCountBeforeAnySend` comparava `nextActionAtMillis` com o prazo da nova publicação. O snapshot lia esse accessor exclusivamente de `nextExecutionAt`, que vale zero durante reserva de publicação. O modelo agora expõe `nextPublicationAtMillis` e `nextExecutionAtMillis` separadamente; `nextActionAtMillis()` permanece como accessor compatível para leitores internos antigos e seleciona o prazo segundo o estado. O teste confirma prazo de publicação futuro e `nextExecutionAtMillis=0` após a reserva.

## Cobertura confirmada

- Kafka real: mensagem versionada, chave `cartId`, campos mínimos, ack confirmado, duplicata explícita, ack perdido com registro ainda consumido, e broker parado resultando em estado incerto.
- Limite de publicação: `send` bloqueado e ack pendente terminam como `UNKNOWN` dentro do orçamento, sem cancelar envio/future; coordenadores concorrentes no Redis autorizam uma única chamada; contagem persiste quando o processo cai depois da reserva e antes do envio.
- O producer usa `acks=all`, idempotência, `linger.ms=0`, `request.timeout.ms=1500`, `delivery.timeout.ms=2000` e `max.block.ms=2000`. O deadline monotônico limita em conjunto a chamada bloqueante `send` e a confirmação; os threads de envio ficam em pool limitado sem fila.
- `SafeKafkaProducerListener` registra somente tópico, chave e motivo fixo; não imprime valor da mensagem nem a exceção bruta.

## Comandos e limites

- Unitários focados: `./mvnw.cmd -pl checkout-worker -am '-Dtest=PublicationCoordinatorTest,KafkaCommandPublisherTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` — 4 testes, zero falhas/erros/skips.
- Gate real: `./mvnw.cmd -pl checkout-worker -am '-Dtest=RedisAttemptStoreIntegrationTest,KafkaCommandPublisherIntegrationTest,PublicationCoordinatorTest,KafkaCommandPublisherTest' '-Dsurefire.failIfNoSpecifiedTests=false' test`, executado pelo script `scripts/verify-kafka-stage.ps1` — 16 testes, zero falhas/erros/skips.

Esta etapa cobre a publicação e sua coordenação, não o processamento Kafka nem offsets; consumo independente, renovação de posse, recuperação agendada e confirmação de offset pertencem à etapa 5. O teste de ack perdido confirma a publicação no tópico, mas não representa uma falha de rede real no caminho de resposta do broker; timeout/bloqueio é coberto separadamente com um future controlado.
