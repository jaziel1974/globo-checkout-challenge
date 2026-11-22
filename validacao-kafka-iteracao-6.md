# Evidências da iteração 6

> Registro histórico: estes gates antecedem a decisão de 22/11/2026 de aguardar Redis e persistir intervenção antes de encaminhar indisponibilidade à DLT. Os resultados abaixo não verificam a nova ordem; a validação atual está registrada no fechamento da iteração 8.

## Gate inicial

O gate real focalizado, executado pelo script `scripts/verify-kafka-stage.ps1` com Kafka e Redis em contêineres, passou com 28 testes, zero falhas, zero erros e zero skips. Os relatórios foram verificados como recentes pelo script. O gate cobriu listener, handler, store Redis e modo CLI.

## Gate adicional de offset e fila de falhas

Após acrescentar um cenário real ao `CheckoutWorkerConsumptionIntegrationTest`, o gate focalizado executou novamente as classes de consumo, store Redis e listener. Resultado reportado pela raiz: **25 testes, zero falhas, zero erros e zero skips; BUILD SUCCESS em 1m21s**, com relatórios frescos. A contagem é uma execução distinta e parcialmente sobreposta ao gate inicial; não se deve somar os dois totais como testes únicos.

O cenário adicional publica registros de falha em Kafka real e simula perda da confirmação retornada pela porta, sem corte de rede: depois de três execuções contadas no Redis e estado `INTERVENCAO`, a resposta simulada permanece `false`, embora a publicação DLT tenha ocorrido. O teste verifica que o offset de origem continua pendente, que a reentrega repete apenas o encaminhamento e não faz uma quarta execução, e que ao liberar a confirmação o offset é confirmado. A possibilidade de registros DLT duplicados é aceita e observada. A execução não simula interrupção física da conexão com o broker.

O gate observou 4/4 testes do `CheckoutWorkerConsumptionIntegrationTest`. Os logs capturados não continham PAN nem CVV de teste. As fontes da v1 e do simulador permaneceram intactas, conforme revisão da raiz.

## Limites registrados

- As verificações reais de indisponibilidade pausaram/restabeleceram Redis, com timeout curto configurado apenas nos testes para caber na janela do teste. A configuração explícita de produção permanece connect timeout de 1 segundo e comando Redis de 2 segundos.
- A perda da confirmação DLT foi simulada na resposta após publicação real; não foi um corte de rede.
- A contagem das verificações locais de Redis pode reiniciar após reinício do processo, conforme a spec.
- A posse expirada foi acelerada em testes anteriores da etapa 5; não constitui medição de espera real de 30 segundos.
- A suíte da etapa 8 e a regressão completa do reactor ainda não foram executadas.
