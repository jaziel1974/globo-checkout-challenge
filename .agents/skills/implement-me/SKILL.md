---
name: implement-me
description: Implementar a spec deste projeto a partir de um plano existente, delegando a execução a um agente econômico. Use quando solicitado para implementar o plano; não use para apenas avaliar o resultado.
---

# Implementar a partir de um plano

Na sessão principal, delegue a execução a um único subagente `implementador_spec`, com `model: gpt-6-luna` e `reasoning_effort: high`. Use a ferramenta de delegação disponível na sessão e passe esses valores explicitamente se ela aceitar seleção de modelo; se oferecer papéis configurados, selecione `implementador_spec`. Não afirme que houve delegação ou que o modelo foi aplicado sem confirmação da ferramenta. Se delegação ou seleção do modelo não estiver disponível, informe o bloqueio; não substitua silenciosamente o agente nem execute o trabalho por conta própria.

Se você já estiver executando com o papel `implementador_spec`, não delegue novamente e não invoque esta skill recursivamente. Execute a implementação diretamente no workspace usando as ferramentas disponíveis. Se esse papel não tiver acesso a arquivos ou terminal, informe o bloqueio à sessão principal.

Se a ferramenta de delegação confirmar o papel e o modelo, mas criar um subagente sem acesso ao workspace, a sessão principal pode usar como fallback o Codex CLI no diretório do projeto com `--model gpt-6-luna` e `model_reasoning_effort=high`. Esse processo é o agente implementador; passe a ele o plano e as mesmas restrições, aguarde sua conclusão e confira as alterações. Não permita que o processo delegue novamente.

Passe ao agente a solicitação do usuário, o caminho do plano fornecido e a raiz do projeto. Se o plano não tiver sido indicado, procure um plano inequívoco no projeto ou na conversa. Se não houver, solicite-o e não invente um. Instrua o agente a ler `spec.md` e o plano integralmente, verificar instruções locais e o estado existente, implementar o plano alinhado à spec, preservar alterações pré-existentes e executar os testes adequados. Se o plano conflitar com a spec, não altere a spec para fazer o código passar: relate a divergência e peça esclarecimento quando não for possível resolvê-la pelo contexto.

Aguarde o resultado do agente, confira as alterações e as evidências relevantes, e informe o que foi implementado, os testes e suas limitações. Não considere uma avaliação independente concluída por esta skill; use `assessment-spec` em etapa separada quando solicitado.
