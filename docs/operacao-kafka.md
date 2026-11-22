# Operação da v2 Kafka

Este guia complementa o [README](../README.md). As garantias e decisões estão na [spec Kafka](../spec_kafka.md) e na [arquitetura Kafka](../arquitetura-kafka.md).

## Processamento e recuperação

O BFF publica em `checkout-v2-commands`; o worker independente processa o comando e grava o resultado no Redis. O BFF consulta esse resultado a cada 100 ms, aguardando até cinco segundos no total, incluindo até dois segundos para confirmar a publicação Kafka (`acks=all`). O limite HTTP não cancela o processamento.

O worker verifica publicações pendentes a cada segundo. Há até três chamadas de publicação pela aplicação, incluindo o envio inicial, com intervalo de 30 segundos. Reenvios internos da biblioteca Kafka ficam sujeitos ao prazo de dois segundos e não contam como novas chamadas. O processamento admite até três execuções por ciclo, separadas por um segundo; o esgotamento exige intervenção.

O consumidor confirma o offset somente após registrar o resultado no Redis ou confirmar o encaminhamento à fila de falhas. Reentregas de tentativas concluídas não executam novamente o pagamento. A reserva técnica dura 30 segundos, é renovada a cada dez segundos e exige a posse atual para concluir a tentativa.

## Inspeção e intervenção

Com os serviços iniciados, execute os comandos abaixo substituindo os argumentos. Cada comando abre um processo curto, sem listener nem scheduler:

```sh
docker compose run --rm worker inspect --attempt-id <uuid>
docker compose run --rm worker authorize-publication --attempt-id <uuid>
docker compose run --rm worker authorize-processing --attempt-id <uuid>
docker compose run --rm worker mark-intervention --attempt-id <uuid> --failure-partition <n> --failure-offset <n>
```

`inspect` mostra estado, contadores, posse e motivo seguro, sem dados do cartão. Antes de autorizar um novo ciclo, inspecione a tentativa e o motivo da falha; não crie outro `attemptId` para contornar o bloqueio.

`authorize-publication` permite um novo ciclo de até três envios do mesmo comando. `authorize-processing` permite um novo ciclo de até três execuções da mesma tentativa. Se a autorização ocorrer antes do intervalo de reenvio, a CLI informa que a publicação ficou agendada. Quando ambos os orçamentos estiverem esgotados, autorize primeiro a publicação e depois o processamento, conforme a saída da CLI. Resultados terminais não são reabertos.

Para consultar a fila de falhas com suas posições:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic checkout-v2-failures --from-beginning --property print.partition=true --property print.offset=true
```

`mark-intervention` valida a posição exata na fila de falhas e confirma a intervenção já persistida. `--failure-partition` e `--failure-offset` são as coordenadas dessa fila; os offsets da mensagem original são independentes. Mensagens inválidas contêm somente metadados seguros: tópico, partição, offset e motivo. Seu reprocessamento exige análise e correção operacional.

## Redis indisponível e fila de falhas

O consumidor faz até três verificações locais, com intervalo de um segundo, sem executar pagamento. Se o Redis continuar indisponível, mantém a origem pendente e aguarda sua recuperação, sem encaminhar à fila de falhas.

Na reentrega, uma tentativa ativa correspondente é bloqueada atomicamente no Redis, com invalidação da posse, antes do encaminhamento. Resultado terminal completo é preservado e permite confirmar a origem. Estado ausente, corrompido, terminal incompleto ou comando divergente são encaminhados com metadados seguros, sem recriar estado nem alterar outra tentativa. Leitura ou mutação Redis incerta mantém a origem pendente. O contador local de verificações pode reiniciar após reinício do worker.

Se a publicação ou confirmação na fila de falhas falhar, a origem permanece pendente. A reentrega repete somente o encaminhamento, sem uma quarta execução automática do pagamento. A fila pode conter duplicatas; a intervenção deve preservar a idempotência.

## Ambiente local e verificações

Os tópicos `checkout-v2-commands` e `checkout-v2-failures` têm uma partição, retenção de sete dias e criação explícita pelo serviço `kafka-init`. O broker usa KRaft, é acessível no host em `localhost:9092` e pelos serviços em `kafka:29092`. Um único broker não oferece redundância, mesmo com `acks=all`.

Redis não tem persistência nem expiração automática dos registros. A perda de seus dados elimina carrinhos, resultados e evidências necessárias à deduplicação; não há recuperação garantida. Não use o mesmo carrinho entre v1 e v2: a proteção entre versões está fora desta entrega.

Os scripts PowerShell abaixo exercitam HTTP, concorrência entre dois BFFs e indisponibilidades controladas. O último faz build das imagens a partir dos JARs previamente compilados, inicia o Compose e inspeciona OpenAPI, tópicos e logs:

```powershell
./scripts/smoke-kafka.ps1
./scripts/smoke-kafka-outages.ps1
./scripts/verify-kafka-compose.ps1
```

Os scripts de falhas pausam e retomam Kafka/Redis locais. Os logs usam identificadores, offsets e motivos, sem corpos de requisição/resposta, número completo de cartão ou CVV.
