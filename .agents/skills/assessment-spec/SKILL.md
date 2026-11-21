---
name: assessment-spec
description: Avaliar independentemente a implementação final deste projeto contra a spec e seus critérios de aceitação, com evidências e testes. Use para auditoria de conformidade; não use para implementar ou corrigir automaticamente.
---

# Avaliar a implementação contra a spec

Delegue a avaliação a um único subagente novo `avaliador_spec`, com `model: gpt-6.1-sol` e `reasoning_effort: high`. Use a ferramenta de delegação disponível na sessão e passe esses valores explicitamente se ela aceitar seleção de modelo; se oferecer papéis configurados, selecione `avaliador_spec`. Não reutilize o agente implementador nem encaminhe sua conclusão como evidência. Não afirme que houve delegação ou que o modelo foi aplicado sem confirmação da ferramenta. Se delegação ou seleção do modelo não estiver disponível, informe o bloqueio; não substitua silenciosamente o agente nem execute a avaliação por conta própria.

Peça ao agente para ler `spec.md` integralmente e avaliar o estado real da implementação. Ele deve extrair todos os requisitos obrigatórios e decisões adotadas, distinguir diferenciais opcionais, mapear cada critério para evidência concreta em código, documentação e testes, e executar verificações pertinentes quando possível. Deve inspecionar os fluxos e testes, incluindo contratos de sucesso e erro, validação, simulação, concorrência, dependências e dados sensíveis, conforme a spec. Não tratar teste aprovado como prova de requisito que ele não exercita.

Solicite uma matriz com critério, status `atendido`, `não atendido` ou `não verificado`, evidências e limitações; achados concretos com severidade e referências; comandos e resultados; e veredito. `Aprovado` exige todos os critérios obrigatórios demonstrados; falha comprovada implica `Reprovado`; evidência insuficiente sem falha comprovada implica `Inconclusivo`. Não altere o código ou a spec durante a avaliação, exceto arquivos temporários necessários aos testes. Aguarde e confira o resultado antes de apresentá-lo ao usuário.
