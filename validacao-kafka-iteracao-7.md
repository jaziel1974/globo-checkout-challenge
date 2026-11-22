# Validação da iteração 7 — API v2

## Resultado

O gate final foi executado pela raiz em 22/11/2026 com `clean verify` e Docker real. Os relatórios Surefire novos somaram 102 testes: simulator 3, BFF 49 e worker 50. Resultado: zero falhas, zero erros, zero skips e `BUILD SUCCESS` em 2m48s. O gate incluiu os testes HTTP v2 com Kafka 4.2.2 e Redis reais, testes de serviço e regressões dos módulos anteriores.

## Cenários HTTP e estado compartilhado

- Sucesso de cartão e Pix, recusa e nova tentativa após recusa, contrato HTTP v1/v2, validação de entrada, tipo de mídia não suportado convertido para `400 / INVALID_REQUEST` pelo handler original e carrinho desconhecido não UUID.
- Concorrência de tentativa ativa e intervenção retornam `409 / PAYMENT_IN_PROGRESS`; o marcador durável de aprovação v2 continua prevalecendo sobre validação posterior, mesmo após a tentativa deixar de ser ativa.
- A espera consulta Redis em intervalos de 100 ms; o teste de resultado registrado mediu resposta dentro da tolerância explícita de 700 ms após concluir o registro.
- Uma mensagem Kafka sem resultado deixa o HTTP terminar em `424` ao redor do prazo global de cinco segundos; concluir a mesma tentativa depois não cancela nem apaga o resultado e nova chamada reconhece a aprovação.
- A perda da resposta Redis foi simulada após uma criação real do Lua no teste de serviço, não em uma chamada HTTP; a aplicação usa `UNKNOWN_APPROVAL` e o handler original responde com o contrato `424` existente, sem publicar quando não confirma a tentativa.
- As propriedades efetivas do produtor v2 foram verificadas: `acks=all`, idempotência, `linger.ms=0`, `request.timeout.ms=1500`, `delivery.timeout.ms=2000` e `max.block.ms=2000`.

## Correções verificadas

O gate inicial da etapa encontrou duas falhas de teste: igualdade de `BigDecimal` que exigia escala 2 apesar de equivalência numérica, e `seekToEnd()` aplicado com posição Kafka ainda lazy, que podia avançar o consumidor além da mensagem recém-publicada. Os testes passaram a comparar o valor decimal numericamente e preservar a posição após o primeiro record.

A investigação revelou também dois casos de comportamento: aprovação v2 durável não era consultada antes da validação de meio/cartão porque `findActiveByCart` deixa de retornar a tentativa após terminal; e timeout/resposta perdida durante a mutação Redis `CREATE` podia ser relatado como falha definitiva. Foi adicionada leitura do marcador aprovado em chave v2 própria e o resultado incerto de `CREATE` passou a usar o erro existente de aprovação desconhecida. Um teste confirma o fechamento seguro se o marcador existir e o registro da tentativa estiver ausente.

Uma falha posterior na fixture de perda de resposta foi causada por `Mockito.spy` em um proxy Spring. Foi substituída por mock delegador ao adaptador real; nenhuma lógica de produção foi alterada para corrigir a fixture.

## Limites

Os testes de API usam Kafka/Redis reais em Testcontainers e completam o resultado através de uma ponte de teste que lê o comando e grava o resultado no Redis; a validação do processo worker independente e do Compose completo pertence à iteração 8. A perda de resposta após mutação Redis é simulada no adaptador de teste, não por corte físico da conexão. Os limites de transmissão e agendamento HTTP são explicitamente tolerados na medição do prazo. A verificação de indisponibilidade via Compose e execução do smoke normal continuam pendentes da iteração 8.

Fontes e testes originais de v1 e do simulador permaneceram intactos. `checkout-bff/src/main/resources/application.yml` recebeu apenas adições de configuração namespaced da v2 e desativação de auto-criação administrativa Kafka; valores v1 existentes foram preservados.
