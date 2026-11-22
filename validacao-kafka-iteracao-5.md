# Validação da iteração 5 — Consumo e confirmação

Data: 22/11/2026.

## Resultado

Etapa aprovada pelo gate `scripts/verify-kafka-stage.ps1` com Docker 27.1.1, Kafka 4.2.2 e Redis reais: 22 testes, zero falhas, zero erros e zero skips; `BUILD SUCCESS` em 1m11s. O verificador confirmou relatórios atuais e sem skips.

## Cobertura confirmada

- O contexto Spring Boot 4.1.2 do worker iniciou com Kafka e Redis reais, usando `spring-boot-starter-kafka` e `spring-boot-starter-jackson` diretos; o worker é não web e depende do classifier `v1-models` sem transitivos do BFF.
- O consumidor processou Pix com o JSON mínimo de quatro campos e cartão com cinco campos. O snapshot de carrinho foi escrito em Redis usando serialização Java das classes `Models` da v1 e lido pelo adaptador do worker; o teste não chamou o endpoint HTTP de pré-checkout, que fica para a iteração 8.
- O teste real esperou o prazo de 30 segundos e confirmou a recuperação agendada de uma tentativa criada apenas no Redis, sem envio inicial ao tópico. A reserva inicial continua independente e pode ocorrer imediatamente; o índice de recuperação só a expõe após o intervalo.
- A simulação de queda processou e persistiu o resultado por um consumidor com auto-commit desabilitado, encerrou sem confirmar o offset e reiniciou o listener real. A redelivery confirmou o offset sem segunda execução. Duplicatas posteriores de Pix e cartão também preservaram `executionCount=1`.
- O gate Redis confirmou posse renovável, takeover após expiração, rejeição de conclusão pelo token obsoleto e corrida atômica entre conclusão e intervenção. O teste de takeover força `leaseUntil=0` para não aguardar 30 segundos; o renovador é testado separadamente com falha controlada.
- A redelivery de tentativa em `INTERVENCAO` não chama o processador, e estado terminal sem resultado não é confirmado.

## Comandos e limites

- Compilação: `./mvnw.cmd -pl checkout-worker -am -DskipTests test-compile` — `BUILD SUCCESS`.
- Gate real focado: `./scripts/verify-kafka-stage.ps1 -TestClasses CheckoutWorkerConsumptionIntegrationTest,PaymentCommandHandlerTest,PaymentCommandTest,ScheduledLeaseRenewerTest,RedisAttemptStoreIntegrationTest` — 22 testes, zero falhas/erros/skips.
- Nenhuma alteração foi feita em `checkout-bff/src` ou `payment-simulator/src`.

O teste de renovação periódica não espera uma janela real de 10 segundos; a política configurada é renovação a cada 10 segundos e posse de 30 segundos. A leitura compatível de snapshot v1 foi demonstrada com objeto serializado no formato real das classes, mas sem criar o carrinho pela chamada HTTP v1.
