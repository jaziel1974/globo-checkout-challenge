# Recuperação do Docker para validação Kafka

Em 22/11/2026, Docker Desktop 4.33.1 / Engine 27.1.1 voltou a responder HTTP 500 após períodos sem contêineres. O log local `com.docker.backend.exe.log` registrou `Idle:paused` às 10:56:21, cinco minutos depois da limpeza dos contêineres do gate da iteração 5. Consultas posteriores registraram `connect tcp 192.168.65.7:2375: no route to host`.

A configuração local continha `useResourceSaver=true` e `autoPauseTimeoutSeconds=300`. O Resource Saver foi desabilitado em `%APPDATA%/Docker/settings.json`, preservando o restante do arquivo e um backup junto ao original (`settings.json.before-kafka-*.bak`). A alteração é local à estação, não é configuração do BFF e não altera contratos ou estado da v1. O Docker documenta a suspensão por inatividade e a opção de desabilitá-la em [Resource Saver](https://docs.docker.com/desktop/use-desktop/resource-saver/).

Essa correlação identifica um gatilho observado; não demonstra, por si só, a eliminação de todas as causas possíveis de HTTP 500. O backend já suspenso continuou indisponível após a alteração, exigindo recuperação. A retomada usa encerramento do Docker Desktop e apenas da distribuição WSL `docker-desktop`, sem desligar outras distribuições nem apagar imagens/volumes.

Às 11:10:24, a verificação de prontidão confirmou Engine 27.1.1 novamente disponível. Às 11:10:31, a leitura da configuração após reinício confirmou `useResourceSaver=false`. A estabilidade além da janela anterior de cinco minutos será observada nos próximos gates; a recuperação não é declarada definitiva apenas por essa consulta.

No gate adicional da iteração 6, iniciado às 12:13, o script encontrou Engine 27.1.1 pronto sem executar recuperação. Esse gate terminou às 12:14:35 com Kafka/Redis reais e 25 testes aprovados, sem skips. É evidência posterior à janela de suspensão anterior, sem garantir ausência de falhas futuras do Docker.

`scripts/verify-kafka-stage.ps1 -EnvironmentOnly -RecoverDocker` verifica/recupera o serviço sem executar Maven. Gates focados usam `-TestClasses`; `-FullSuite` executa `clean verify` no reactor completo. O verificador recusa aprovação com relatórios ausentes, antigos, falhas, erros ou testes ignorados. Os resultados efetivos de cada etapa continuam nos relatórios de validação das iterações.

Na retomada às 17:45, o Docker Desktop estava parado e o named pipe do Engine indisponível. A recuperação pelo modo `-EnvironmentOnly -RecoverDocker` confirmou Engine 27.1.1 pronto às 17:50, sem apagar imagens ou volumes. O `clean verify` subsequente terminou às 18:01:32 com 107 testes, sem falhas, erros ou skips.

A validação final do Compose terminou às 18:30 com os JARs desse gate, fluxos HTTP e concorrência entre dois BFFs aprovados, incluindo pausa e retomada do Kafka e do Redis. O healthcheck Redis ficou temporariamente `unhealthy` após a pausa deliberada; uma consulta posterior confirmou Kafka e Redis `healthy` e os cinco serviços principais em execução. Os logs brutos locais estão em `validacao-kafka-compose-final.log` e `validacao-kafka-compose-servicos.log`, ignorados pelo Git; o resultado e seus limites ficam registrados em `validacao-kafka-iteracao-8.md`.
