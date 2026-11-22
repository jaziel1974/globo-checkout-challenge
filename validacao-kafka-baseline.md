# Validação de baseline — preparação para Kafka

Data: 21/11/2026. Esta validação cobre a implementação v1 existente e um broker Kafka isolado; a implementação v2 ainda não existe e não foi avaliada.

## Ambiente

- Java: Eclipse Temurin OpenJDK 21.0.13.
- Docker Desktop iniciado; servidor Docker 27.1.1 acessível. Os comandos que precisaram do serviço foram executados com acesso ao Docker fora do ambiente restrito.
- O projeto contém alterações não rastreadas nos documentos Kafka; nenhum arquivo de código v1 foi alterado nesta validação.

## Baseline da v1

- `./mvnw.cmd clean verify`: **BUILD SUCCESS**, 42 testes executados, zero falhas, zero erros e zero testes ignorados: três no simulador e 39 no BFF.
- Os quatro testes de integração Redis foram executados com Redis real em contêiner Testcontainers, incluindo concorrência e resultado de aprovação incerto.
- Compilação exibiu avisos existentes de APIs depreciadas, operações não verificadas e carregamento dinâmico do agente Mockito; não impediram o build. Não foram alterados nesta etapa.

## Verificação isolada do Kafka

- Imagem `apache/kafka:4.2.2` obtida do registry; digest `sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69`.
- Contêiner temporário iniciado com `KAFKA_OPTS=-XX:-UseBASE64Intrinsics`; broker acessível por `localhost:9092` no próprio contêiner.
- Criados `checkout-v2-commands` e `checkout-v2-failures`, cada um com uma partição, fator de replicação um, `min.insync.replicas=1`, `cleanup.policy=delete`, `retention.ms=604800000` e `retention.bytes=-1`. Os valores de configuração foram consultados no broker.
- Mensagem sintética publicada com `acks=all` e consumida do tópico de comandos. O contêiner temporário foi encerrado após o teste.
- Artefato `org.springframework.kafka:spring-kafka:4.1.2` obtido do Maven Central. Uma primeira chamada de `dependency:get` falhou por interpretação de argumentos no PowerShell; a execução com o argumento `-Dartifact` entre aspas teve sucesso.

## Limites

- O teste isolado não comprova a configuração do futuro Docker Compose, nem o fluxo HTTP v2, retries, concorrência, offset, fila de falhas ou intervenção operacional; esses testes integram as próximas iterações do plano.
- A disponibilidade da biblioteca e da imagem não comprova compatibilidade de toda a aplicação. O classifier Maven, o consumidor e os comandos operacionais previstos ainda precisam ser implementados e validados.
- A proteção contra uso do mesmo carrinho nas duas versões não faz parte do escopo aprovado. Redis permanece sem persistência.
