# Plano de implementação — checkout v2 com Kafka

## Referências e estado

Plano de desenvolvimento baseado em [spec_kafka.md](spec_kafka.md), [spec.md](spec.md), [arquitetura-kafka.md](arquitetura-kafka.md) e na implementação atual. O pré-checkout e a implementação da v1 devem permanecer inalterados. Este plano não autoriza alterar contratos ou regras acordadas para contornar falhas.

A implementação começa pela iteração 0 e só avança após validar cada condição de saída; qualquer lacuna que exija alteração de comportamento público deve ser resolvida com o usuário antes da implementação afetada. A integração Kafka é assíncrona internamente; `POST /v2/operations/checkout` aguarda resultado e devolve os mesmos contratos v1, com a exceção `409 / PAYMENT_IN_PROGRESS` para outra tentativa v2 em andamento e o `424` existente para resultado desconhecido. O prazo é de cinco segundos no total, incluindo até dois segundos para confirmação da publicação.

Cada cliente usa uma versão por definição; não haverá nesta etapa safeguard que impeça uso simultâneo de versões. A proteção de duplicidade e os testes deste plano abrangem a v2 entre suas instâncias e reentregas, sem afirmar garantia entre versões. Redis não é persistente, e sua perda limita recuperação e idempotência.

## Regra de validação por iteração

Cada etapa abaixo é uma iteração independente com **entregáveis, verificação e condição de saída**. Antes de avançar, executar os testes ou verificações indicados para a etapa e revisar os critérios, sem aguardar aprovação manual do usuário, conforme sua escolha; registrar comandos, resultados, falhas e limitações. Se houver falha, corrigi-la e repetir a validação afetada; se houver impossibilidade ou decisão de contrato não resolvida, registrar o bloqueio em vez de declarar aprovação. Não tratar testes pulados como aprovados. Preservar alterações existentes e não fazer commit ou publicação sem instrução.

## Iteração 0 — Fechar desenho e baseline

1. Conferir `spec_kafka.md`, documentos originais e estado do workspace; identificar trechos v1 que devem permanecer inalterados.
2. Revisar as operações atômicas, tópicos, descarte de mensagens e ferramentas de intervenção em `arquitetura-kafka.md`. Preservar as decisões já registradas para aprovação em chaves v2, prazo, estados internos e correlação após `424`; registrar eventuais lacunas e limites.
3. Descrever os cenários de falha entre gravação, envio, confirmação, consumo, resultado e resposta, inclusive queda do BFF, reentrega e recusa seguida de nova tentativa.
4. Registrar a versão de Java, disponibilidade de Docker, Redis e Kafka e comando de baseline da suíte existente.

**Validação:** revisar o desenho contra cada requisito e cenário; executar `mvnw clean verify` com Java 21 e Docker, registrando falhas/skips e o estado anterior à mudança. Verificar que v1 e pré-checkout não são alterados pelo desenho.

**Saída:** decisões e cenários rastreáveis, baseline conhecido e nenhuma incompatibilidade de contrato sem resolução.

**Baseline executado em 21/11/2026:** [resultado e limites](validacao-kafka-baseline.md). A suíte existente passou com 42 testes e sem testes ignorados; foi também realizada uma verificação isolada do broker e dos tópicos. Essa evidência não substitui a validação das iterações de implementação.

Na recuperação após queda com publicação não confirmada, o usuário escolheu retomada automática após 30 segundos, substituindo a escolha anterior de depender exclusivamente de repetição do cliente. Essa recuperação se limita à publicação não confirmada, sem reabrir tentativas concluídas ou esgotadas. Guardar no Redis somente o comando mínimo aprovado para Kafka e o mesmo identificador interno. Se o cliente repetir, retornar `409` e permitir a recuperação segura da tentativa já registrada, sem interpretar dados diferentes como autorização para substituí-la. O controle de concorrência e seus limites estão registrados em `arquitetura-kafka.md` e devem ser validados antes de implementar. Tentativas na fila de falhas continuam aguardando intervenção, com `409` para novas chamadas.

A publicação terá até três tentativas no total, incluindo o primeiro envio, com intervalo de 30 segundos. Conta-se cada chamada da aplicação ao produtor; reenvios internos da biblioteca devem ser documentados. A contagem no Redis será incrementada antes de cada envio; uma queda após o registro consome a tentativa. O consumidor Kafka independente verificará pendências a cada segundo por agendamento próprio e coordenação atômica no Redis, sem depender da chegada de mensagens; não haverá reenvio enquanto o consumidor processar. Após esgotamento, exige-se intervenção e novas chamadas recebem `409`; mensagem recebida após a marcação de esgotamento vai à fila de falhas sem executar o pagamento. Se o processamento estiver em curso, a marcação invalida a posse e impede conclusão; não desfaz resultado já gravado. A operação pode autorizar novo ciclo de até três envios da mesma tentativa. Não reenviar enquanto o estado Redis não puder ser confirmado. Uma nova chamada não ultrapassará esses limites. A espera HTTP consultará Redis a cada 100 ms.

## Iteração 1 — Limites, contratos e módulos

1. Definir estruturas de comando versionado e resultado interno com os campos mínimos da spec, sem PAN completo/CVV.
2. Definir portas de aplicação para publicação, estado/posse e resultado; separar regra de negócio do transporte e da persistência.
3. Criar a estrutura necessária para o consumidor independente e a configuração dos módulos Maven, evitando dependência circular e alteração da v1. Gerar o classifier `v1-models` e utilizá-lo no consumidor conforme a arquitetura para ler o `payload` Java serializado que o pré-checkout grava no Redis, sem assumir JSON.

**Validação:** compilar o reactor a partir do zero e testar que o classifier é produzido e resolvido antes dos testes; testar serialização e validação do comando para cartão e Pix, formato inválido e versão desconhecida; conferir que o contrato Kafka não contém dados proibidos.

**Saída:** módulos compilam, limites são testáveis isoladamente e v1 permanece inalterada.

## Iteração 2 — Estado compartilhado e reserva v2

1. Implementar adaptador de estado das tentativas com identificação interna, estados `AGUARDANDO_PUBLICACAO`, `PUBLICADO`, `PROCESSANDO`, `APROVADO`, `RECUSADO` e `INTERVENCAO` em chaves próprias da v2 e operações atômicas no Redis para criação, leitura, concorrência v2 e índice ordenado por próxima recuperação, sem mudar o repositório ou estado v1. Tentativas e resultados não expiram automaticamente.
2. Implementar posse de 30 segundos, renovação a cada 10 segundos, identificador único por aquisição e comparação atômica da posse no término; a expiração da posse não apaga a tentativa.
3. Registrar contagem antes de cada execução e impedir execução acima de três por ciclo enquanto o estado estiver preservado.

**Validação:** testes de integração com Redis real para criação concorrente, conflito v2, índice e estado atualizados atomicamente, confirmação de publicação tardia sem regressão de estado, ausência de expiração de tentativa, expiração e renovação de posse, retomada da mesma tentativa, identificador de posse substituído, bloqueio de finalização tardia, contagem e ausência de PAN/CVV. Testar perda de estado e registrar o limite de recuperação; verificar que as chaves e o estado v1 não são alterados pela gestão dos estados técnicos v2.

**Saída:** invariantes atômicas de estado e posse passam em testes; a ausência de reexecução pelo consumidor completo será verificada na iteração 5.

**Fechamento em 21/11/2026:** iteração 2 validada e aprovada. O gate `./mvnw.cmd -pl checkout-worker -am '-Dtest=RedisAttemptStoreIntegrationTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` passou com Redis real: 9 testes, zero falhas, zero erros e zero skips. Após corrigir a precedência do limite de três execuções sobre `nextExecutionAt`, `./mvnw.cmd clean verify` passou com Docker real: 55 testes no reactor, zero falhas, zero erros e zero skips. Evidências e limites: [validacao-kafka-iteracao-2.md](validacao-kafka-iteracao-2.md). A renovação periódica da posse pelo consumidor e o processamento em processo independente continuam para a iteração 5.

## Iteração 3 — Simulação e resultados

1. Aplicar validações e regras de `spec.md` na v2 sem alterar a v1: carrinho, meio, cartão, Pix e snapshot.
2. Executar a regra de recusa `0002` com os quatro últimos dígitos do comando e calcular o total decimal a partir do snapshot.
3. Armazenar o resultado interno mínimo necessário para reproduzir a resposta v1 e transicionar o estado de forma segura, respeitando a posse. Registrar a aprovação por carrinho em chave própria v2, de modo atômico com a conclusão, sem modificar o carrinho v1.

**Validação:** testes de cartão aprovado/recusado, crédito/débito, Pix, snapshot e soma; garantir nova tentativa após recusa, pagamento já aprovado na v2 e ausência de dados sensíveis. Verificar que aprovação v2 persiste em chave própria e não altera o `status` do carrinho da v1; não interpretar isso como proteção entre versões.

**Saída:** resultados e erros de negócio correspondem aos da v1, com a mesma estrutura e códigos.

**Fechamento em 22/11/2026:** gate `./mvnw.cmd -pl checkout-worker -am test` com Docker/Redis real passou: BFF 39 + worker 20 = 59 testes, zero falhas, zero erros, zero skips; `BUILD SUCCESS` em 1m08s. O gate unitário focado também passou anteriormente com 11 testes. Os testes cobrem simulação de cartão/Pix, total a partir do snapshot, validação e leitura do payload Java serializado; o adaptador real de estado foi incluído na regressão Redis. A aprovação v2 é armazenada em chave própria e o teste garante que o status do carrinho v1 não seja alterado. Evidências: [validacao-kafka-iteracao-3.md](validacao-kafka-iteracao-3.md). Limite: recusa seguida de nova tentativa e execução Kafka independente ainda serão integradas nas iterações posteriores.

## Iteração 4 — Publicação Kafka

1. Implementar o adaptador da porta de publicação com mensagem versionada, `acks=all` e confirmação em até dois segundos.
2. Diferenciar confirmação, falha conhecida e resultado incerto; não presumir que timeout cancela o envio.
3. Conectar publicação ao estado interno conforme o desenho aprovado, sem exigir cabeçalho adicional do frontend.

**Validação:** testes com Kafka real para publicação, campos, broker indisponível, confirmação perdida e duplicata; queda após incrementar a contagem e antes de enviar; concorrência de recuperadores e três envios incluindo o inicial. Exercitar a coordenação com testes dos adaptadores disponíveis nesta etapa. Os cenários que exigem consumidor completo, API HTTP ou intervenção operacional são validados nas iterações 5 a 7, sem declarar sua aprovação antecipadamente. Verificar que o prazo de publicação respeita o orçamento recebido e que a confirmação tardia não regride o estado.

**Saída:** comando publicado e falhas classificadas de forma reproduzível; dados proibidos não aparecem no Kafka.

**Fechamento em 22/11/2026:** gate focado do script `scripts/verify-kafka-stage.ps1` passou com Docker 27.1.1, Kafka 4.2.2 e Redis reais: 16 testes, zero falhas, erros ou skips; `BUILD SUCCESS` em 42.236s, com relatórios frescos confirmados pelo verificador. A tentativa anterior teve uma falha em `applicationCrashAfterReservationConsumesCountBeforeAnySend`: o modelo unificava prazos e expunha `nextExecutionAt=0` como prazo da publicação; a correção separou `nextPublicationAtMillis` e `nextExecutionAtMillis` no snapshot e preservou um accessor interno compatível. Evidência: [validacao-kafka-iteracao-4.md](validacao-kafka-iteracao-4.md). Foram verificados publicação e chave/corpo mínimo, duplicata, ack perdido sem cancelamento, broker indisponível, deadline com envio bloqueado, reserva concorrente e contagem persistida antes de envio. Falhas do producer são registradas por listener seguro sem imprimir corpo nem exceção.

## Iteração 5 — Consumo e confirmação

1. Implementar consumidor em processo independente; validar schema e campos antes de executar o pagamento.
2. Adquirir/renovar posse, chamar a aplicação por porta e concluir somente com posse válida.
3. Confirmar offset após resultado registrado; ao reencontrar tentativa concluída, confirmar sem reexecutar.

**Validação:** testes Kafka/Redis reais com consumo de cartão e Pix, leitura pelo consumidor de snapshot criado pelo pré-checkout v1, reentrega, queda após resultado e antes do offset, perda de posse e retomada por outra instância; recuperação agendada mesmo sem mensagem no tópico; mensagem após esgotamento e corrida entre esgotamento e conclusão. Comparar efeitos no Redis e offset, não apenas respostas dos mocks.

**Saída:** uma única aprovação no fluxo v2 com estado preservado; consumidor duplicado ou atrasado não conclui novamente.

**Fechamento em 22/11/2026:** gate real aprovado com Kafka 4.2.2 e Redis: 22 testes, zero falhas, erros ou skips, `BUILD SUCCESS` em 1m11s. Incluiu startup do worker, snapshot v1 serializado, Pix/cartão, recuperação agendada após 30 segundos, redelivery sem commit e posse obsoleta. Limites: a criação do snapshot não passou pela chamada HTTP de pré-checkout e a expiração da posse foi acelerada controlando `leaseUntil=0`. Evidências: [validacao-kafka-iteracao-5.md](validacao-kafka-iteracao-5.md).

## Iteração 6 — Retries, fila de falhas e operação

1. Aplicar três execuções com intervalo de um segundo e encaminhar esgotamento à fila de falhas mantendo a tentativa para intervenção.
2. Não confirmar o offset se o envio à fila falhar; permitir novo encaminhamento sem quarta execução automática e suportar duplicatas na fila.
3. Tratar Redis indisponível com até três verificações locais sem pagamento; documentar reinício da contagem local após queda.
4. Encaminhar mensagem inválida sem copiar corpo desconhecido; rejeitar mensagens antigas sem estado Redis. Implementar a CLI de inspeção e autorização planejada na arquitetura e o reprocessamento manual da mesma tentativa.

**Validação histórica executada antes da decisão atual (superada):** os gates então vigentes validaram tentativas, intervalo, falha/duplicata da fila, reinício, mensagens inválidas, perda de estado e autorização manual; o cenário de Redis indisponível encaminhava à DLT e depois executava `mark-intervention`. Esse fluxo foi substituído pela decisão operacional registrada na iteração 8. Os gates históricos não são evidência da ordem Redis→intervenção→DLT atualmente aprovada; a decisão atual exige validação própria no fechamento da iteração 8.

**Saída:** limites, offset e intervenção funcionam conforme a spec e podem ser executados pelo procedimento documentado.

  **Fechamento em 22/11/2026:** etapa validada em dois gates focados com Kafka e Redis reais. O primeiro passou com 28 testes; após acrescentar o cenário de confirmação DLT perdida, o segundo passou com 25 testes, zero falhas, erros ou skips e `BUILD SUCCESS` em 1m21s. Os totais são de execuções parcialmente sobrepostas e não representam 53 testes únicos. O cenário final publicou a DLT real, simulou resposta de confirmação perdida, confirmou offset de origem pendente, repetiu o encaminhamento sem quarta execução e confirmou o offset depois que a resposta foi liberada. Limite: a perda foi simulada na resposta da porta após publicar, não por corte físico de rede. Evidências e limitações: [validacao-kafka-iteracao-6.md](validacao-kafka-iteracao-6.md).

## Iteração 7 — API v2 e prazo total

1. Expor `POST /v2/operations/checkout` com entrada e validação v1, sem chave adicional no frontend.
2. Conectar aplicação v2, publicação e espera pelo resultado por consultas ao Redis a cada 100 ms; aplicar deadline global de cinco segundos, incluindo publicação, e verificar sob múltiplas instâncias.
3. Retornar JSONs e HTTP originais de sucesso e erro, `409 / PAYMENT_IN_PROGRESS` para outra tentativa v2 e `424` original de resultado desconhecido quando aplicável.

**Validação:** testes HTTP comparativos v1/v2 para sucesso cartão/Pix, recusa e todos os erros existentes; concorrência v2 e `409` inclusive após esgotamento; publicação atrasada, Redis indisponível e resultado tardio; medir deadline com tolerância de transmissão/agendamento explicitada. Verificar que `424` não dispara cancelamento implícito nem segunda aprovação.

**Saída:** contrato público aprovado por evidência HTTP e nenhum arquivo de implementação v1 alterado.

**Fechamento em 22/11/2026:** `clean verify` com Docker real passou: 102 testes nos módulos simulator (3), BFF (49) e worker (50), zero falhas, erros ou skips, `BUILD SUCCESS` em 2m48s. O gate cobre respostas HTTP v1/v2, tentativa simultânea, intervenção, resultado tardio após 424, deadline global de cinco segundos, polling, propriedades efetivas Kafka e recuperação de resultado Redis incerto. Evidências, correções do gate inicial e limites: [validacao-kafka-iteracao-7.md](validacao-kafka-iteracao-7.md). Fontes/regras/testes originais de v1 e simulator estão intactos; `application.yml` recebeu adições de configuração exclusiva v2.

## Iteração 8 — Infraestrutura e fluxo integrado

1. Incluir `apache/kafka:4.2.2` e consumidor independente no Docker Compose, com configuração de conectividade, mitigação da JVM, prontidão e tópicos definida na arquitetura, inclusive retenção de sete dias em ambos; usar versões Kafka e Testcontainers especificadas no desenho.
2. Atualizar README e OpenAPI para v1/v2, execução, limites, simulação, incidentes, logs e intervenção. Documentar que não há safeguard entre versões nesta etapa e que um broker não oferece redundância.

**Decisão operacional registrada em 22/11/2026:** aguardar Redis e persistir o bloqueio antes de encaminhar à fila de falhas. A DLT não é confirmada enquanto Redis estiver indisponível; a mensagem de origem permanece pendente. Na redelivery após o limite local, persistir `INTERVENCAO/REDIS_UNAVAILABLE` com fencing antes da publicação DLT. Um terminal já confirmado é preservado e permite confirmar a origem sem intervenção. Se Redis ou a confirmação da mutação forem incertos, não presumir que a transição foi gravada.

**Validação:** build limpo e `docker compose up --build`; inspecionar a configuração efetiva dos dois tópicos, inclusive retenção; executar pré-checkout e v2 em cartão aprovado, cartão recusado e Pix; testar broker e Redis indisponíveis. Com Redis pausado, confirmar que a origem permanece pendente e que nenhuma DLT nem processamento ocorre; restaurar Redis, observar bloqueio persistido/fencing, depois DLT real e confirmação da origem. Cobrir falha/confirmação DLT perdida com repetição de encaminhamento sem nova execução, estado terminal preservado, e processo/store reiniciado lendo estado Redis compartilhado. A antiga validação de checkpoint/reconciliação DLT não se aplica à decisão atual.

**Fechamento em 22/11/2026:** `clean verify` passou com 107 testes (simulator 3, BFF 49, worker 55), zero falhas/erros/skips e `BUILD SUCCESS` em 3m12s às 18:01:32. `scripts/verify-kafka-compose.ps1` terminou exit 0 após rebuild dos JARs finais. Foram verificados OpenAPI servido, retenção efetiva de sete dias nos dois tópicos, nove smokes normais (incluindo concorrência 200/409 entre dois BFFs), panes Kafka e Redis com recuperação, além de logs sem PAN/CVV completo. Evidências e limites: [validacao-kafka-iteracao-8.md](validacao-kafka-iteracao-8.md), [log do helper](validacao-kafka-compose-final.log) e [serviços/logs](validacao-kafka-compose-servicos.log). Nos Testcontainers do worker, o teste da pane Redis prova as três verificações locais e bloqueio persistido antes da DLT; o smoke Compose mede apenas o contrato HTTP e retomada após Redis. Redis é volátil, há um broker e a saúde reportou `unhealthy` transitoriamente após pause, embora a recuperação HTTP subsequente tenha passado.

**Saída:** fluxo local reproduzível e instruções consistentes com a execução real.

## Iteração 9 — Regressão e avaliação independente

1. Executar suíte completa com Java 21 e Docker, incluindo testes reais Redis/Kafka; revisar se os testes existentes da v1 não foram enfraquecidos.
2. Solicitar avaliação independente contra `spec.md` e `spec_kafka.md`; registrar critérios atendidos, falhas e itens não verificados com evidências.
3. Corrigir problemas no escopo e repetir as validações afetadas; nova avaliação independente antes de declarar conformidade.

**Validação:** `mvnw clean verify`, testes integrados e avaliação independente; nenhum critério obrigatório faltante ou teste necessário ignorado. Registrar limitações da configuração local e do Redis volátil.

**Fechamento em 22/11/2026:** a regressão `clean verify` passou com 107 testes, zero falhas/erros/skips; a validação Compose da iteração 8 passou e a avaliação independente final concluiu que a implementação local foi aprovada, sem achado obrigatório conhecido em aberto, com A1–A5 corrigidos/demonstrados. A entrega global permanece inconclusiva porque a publicação do estado Kafka no repositório público não foi solicitada nem demonstrada; commit/publicação não foram executados. Ver [validacao-kafka-iteracao-9.md](validacao-kafka-iteracao-9.md) e [avaliacao-kafka-final.md](avaliacao-kafka-final.md). Essa pendência externa não invalida o resultado funcional local.

**Saída:** evidências completas de conformidade ou relatório claro de bloqueios, sem afirmar aprovação com verificação insuficiente.

## Fora do escopo

Não implementar nesta etapa identificação de cliente ou safeguard de uso simultâneo de v1/v2, garantias entre versões, cache de produtos/meios, PostgreSQL ou MongoDB. Não mudar regras de negócio existentes nem enviar ou persistir PAN completo/CVV.
