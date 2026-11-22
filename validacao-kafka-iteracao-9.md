# Validação da iteração 9 — regressão e avaliação independente

## Regressão Maven

Em 22/11/2026, o `clean verify` final executado pela raiz passou com Docker real: 107 testes — simulator 3, BFF 49 e worker 55 —, zero falhas, zero erros e zero skips; `BUILD SUCCESS` em 3m12s, encerrado às 18:01:32. Os testes originais da v1 e do simulator foram incluídos no reactor; a implementação e os contratos v1 permaneceram preservados.

O gate imediatamente anterior encontrou uma falha apenas na sincronização da fixture Redis-outage; ela foi corrigida no teste para aguardar evidência das três verificações e da resolução Redis falha no registro específico, sem alterar produção. O gate final passou após essa correção.

## Avaliação independente e fechamento

A validação Compose da iteração 8 foi concluída com exit code 0; ver [validacao-kafka-iteracao-8.md](validacao-kafka-iteracao-8.md) e os logs finais referenciados ali.

O relatório [avaliação independente final](avaliacao-kafka-final.md) concluiu: “**Aprovado no escopo autorizado de implementação e validação no workspace. Inconclusivo quanto à entrega global completa**”. Os critérios obrigatórios de implementação local foram atendidos, A1–A5 foram corrigidos e demonstrados, e não há achado obrigatório conhecido em aberto. O veredito global permanece inconclusivo exclusivamente porque a publicação do estado Kafka no repositório público não foi solicitada nem demonstrada. Commit e publicação não foram executados.

Assim, a iteração 9 está fechada para o escopo autorizado local. A pendência de publicação é externa à conformidade funcional avaliada e permanece explicitamente registrada; não é tratada como falha de código.

## Limites

O gate Maven não prova por si só comportamento de deployment fora do Compose local, persistência Redis (desativada por desenho), redundância Kafka (um broker) nem recuperação após perda integral do estado Redis. Esses limites devem permanecer explícitos no relatório de fechamento.
