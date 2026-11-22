# Validação — iteração 2 do checkout v2 com Kafka

Data: 21/11/2026. Escopo: estado de tentativas v2, concorrência por carrinho, índice de recuperação, posse, contagem de publicação e execução, conclusão e proteção do estado v1.

## Ambiente e comando

- Java Temurin 21.0.13; Docker Desktop conectado, servidor 27.1.1.
- `./mvnw.cmd -pl checkout-worker -am '-Dtest=RedisAttemptStoreIntegrationTest' '-Dsurefire.failIfNoSpecifiedTests=false' test`
- Resultado final: **BUILD SUCCESS**; nove testes de integração com Redis real, zero falhas, zero erros e zero skips (16,556 s).
- Regressão final: `./mvnw.cmd clean verify` — **BUILD SUCCESS** com Docker real; simulator 3 + BFF 39 + worker 13 = 55 testes, zero falhas, zero erros e zero skips (1m10s).
- Compilação anterior: `./mvnw.cmd -pl checkout-worker -am -DskipTests test-compile` — **BUILD SUCCESS**.

## Falha inicial e correção

A primeira execução do gate usou Redis real e executou sete testes; seis passaram e um falhou. Em `possessionRenewalReplacementAndThreeExecutionLimitAreAtomic`, a quarta reserva de execução retornou `Optional(0)` porque o script verificava o intervalo antes de verificar o limite já esgotado. O script Lua foi corrigido para verificar primeiro se as três execuções do ciclo já foram consumidas e só então retornar “ainda não elegível” para uma tentativa cujo orçamento não se esgotou. O gate seguinte incluiu nove testes e passou sem skips.

Depois do gate focado, a suíte completa foi executada com Docker real e passou sem skips; não houve alterações nos fontes ou testes do BFF v1 nem do simulador.

## Cobertura observada

- Criação concorrente por duas instâncias do adaptador: apenas uma tentativa ativa vence; o vínculo e a entrada inicial do índice são criados juntos.
- Um vínculo ativo sem registro correspondente falha fechado e não é substituído.
- Reserva de publicação contabiliza atomicamente antes do envio, respeita o intervalo e encerra o ciclo após três reservas; o esgotamento remove a entrada do índice e mantém a tentativa em intervenção.
- Confirmação tardia remove a recuperação pendente sem regredir `PROCESSANDO` ou um resultado terminal.
- Posse de 30 segundos impede aquisição concorrente, renova enquanto válida e permite retomar a mesma tentativa após vencimento simulado; o token antigo não renova nem conclui.
- As três execuções são contadas; a quarta é recusada mesmo antes do próximo instante elegível.
- Esgotamento de publicação e conclusão foram exercitados nos dois ordenamentos: se a intervenção vence, o detentor não grava resultado; se a conclusão vence, a aprovação permanece e o esgotamento não reabre a tentativa.
- A conclusão exige que a tentativa ainda possua o vínculo ativo do carrinho e grava resultado, vínculo aprovado e estado terminal atomicamente. A recusa remove o vínculo ativo e permite criar nova tentativa.
- Tentativas não têm TTL; o estado v1 do carrinho continua `PENDENTE`; os dados guardados no comando não incluem número completo de cartão nem CVV.
- Após `FLUSHDB`, o estado antigo não é recriado: a tentativa deixa de existir e um checkout sem carrinho retorna `CART_NOT_FOUND`.

## Limites desta iteração

- O código define a posse por 30.000 ms. O teste altera `leaseUntil` no Redis para simular o vencimento e comprovar a troca de token; não aguarda 30 segundos de relógio real. A renovação foi chamada diretamente dentro do prazo, sem validar a cadência periódica de 10 segundos, que será implementada e testada pelo consumidor na iteração 5.
- A concorrência usa duas instâncias do adaptador em um único processo de teste contra Redis real; a execução de consumidores em processos separados também fica para iteração 5.
- A perda completa do Redis foi representada por `FLUSHDB`, confirmando que carrinhos e tentativas necessários não podem ser recuperados depois da perda. Isso não representa um mecanismo de persistência ou recuperação.
- As configurações e o Compose Kafka, o fluxo HTTP v2 e a operação manual permanecem para iterações posteriores.
