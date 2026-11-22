# Validação da iteração 8 — infraestrutura e fluxo integrado

## Gate Maven

Em 22/11/2026, o `clean verify` final executado pela raiz passou com Docker real: 107 testes — simulator 3, BFF 49 e worker 55 —, zero falhas, zero erros e zero skips; `BUILD SUCCESS` em 3m12s, encerrado às 18:01:32. Os relatórios foram regenerados para esse gate.

O gate inicial das alterações finais falhou em `CheckoutWorkerConsumptionIntegrationTest.redisOutageWaitsToPersistInterventionBeforeForwarding`: a fixture pausava Redis e esperava quatro segundos sem garantir que o consumidor tivesse alcançado as três verificações locais do registro-alvo. A correção posicionou o offset do grupo no registro enviado e sincronizou o teste com três retornos `REDIS_UNAVAILABLE` daquele comando e a falha de resolução enquanto Redis ainda estava pausado. Não houve alteração da regra de produção. O gate final seguinte passou.

## Compose e smokes

Em 22/11/2026, `scripts/verify-kafka-compose.ps1` terminou com exit code 0 após rebuild e inicialização dos JARs finais. As evidências de execução estão em [validacao-kafka-compose-final.log](validacao-kafka-compose-final.log) e [validacao-kafka-compose-servicos.log](validacao-kafka-compose-servicos.log).

- O OpenAPI gerado respondeu com v1 e pré-checkout preservados e respostas v2 diretas `200/400/404/409/422/424`.
- A configuração Kafka efetiva confirmou `retention.ms=604800000` e `cleanup.policy=delete` nos tópicos de comandos e falhas; broker único e `min.insync.replicas=1`.
- Nove cenários normais passaram: quatro cenários v1, quatro v2 e concorrência entre dois BFFs (uma resposta `200`, outra `409`). Cartão aprovado/duplicata, recusa seguida de nova tentativa, Pix e débito com snapshot de três produtos foram cobertos.
- No smoke de pane, Kafka pausado produziu `424/409` na v2 enquanto v1 respondeu `200` (5.010 ms); após retomada, a mesma tentativa foi reconhecida como aprovada (`409 / PAYMENT_ALREADY_APPROVED`, 17 ms). Redis pausado produziu `424` em 5.021 ms; após restaurar Redis, uma nova chamada teve sucesso e o script verificou que não houve criação tardia associada à chamada que expirou.
- Logs de serviço foram inspecionados e não continham os números completos de cartão ou CVVs das fixtures. A captura de status incluída ao final do helper, às 18:30:19, mostrou Redis temporariamente `unhealthy` após pause/unpause; uma consulta posterior `docker compose ps` confirmou os cinco serviços principais em execução e Kafka/Redis saudáveis. O histórico de recuperação do Docker está em [validacao-docker-kafka.md](validacao-docker-kafka.md).

O cenário de indisponibilidade Redis no Compose valida o limite HTTP e a retomada. A ordem específica de três verificações locais, intervenção Redis persistida com fencing e somente depois publicação DLT é provada pelos testes de integração Redis/Kafka do worker no gate `clean verify`; não atribuir essa sequência ao smoke de produtor HTTP.

## Limites

O Compose local usa um broker e Redis volátil; a retenção Kafka verificada não equivale a redundância nem a persistência do Redis. Nenhum teste esperou sete dias naturais para observar a expiração de mensagens.
