# Plano de desenvolvimento — BFF de checkout

## Referências e escopo

- Requisitos e contratos: [spec.md](spec.md).
- Componentes e decisões arquiteturais: [arquitetura.md](arquitetura.md).
- Documento original: `CKW-Desafio Backend-141026-171112 3 (1).pdf`.

Este plano prepara a implementação com `$implement-me` e a avaliação independente com `$assessment-spec`. Caso surja uma ambiguidade que altere contratos ou comportamento, resolvê-la com o usuário; não atribuir uma solução à spec por suposição. Não publicar o repositório nem fazer commit sem instrução específica.

## Sequência de trabalho

### 1. Base do projeto

- Preparar um projeto Maven com Wrapper e dois módulos no mesmo repositório: BFF e simulador HTTP, usando Java 21 e Spring Boot 4.1.2.
- Configurar a execução local com Docker Compose para BFF, simulador e Redis compartilhado sem persistência; configurar URLs e conexão Redis sem fixar endereços de ambiente no código.
- Organizar o BFF por API, aplicação/regras, domínio e infraestrutura, mantendo dependências acessadas por interfaces próprias.
- Configurar JUnit, Mockito e Testcontainers. Documentar pré-requisitos e comandos efetivamente suportados no README.

### 2. Simulador e clientes

- Implementar o catálogo e os meios de pagamento especificados, preservando seus textos, preços e moeda. O simulador atende `GET /products/{slug}` e `GET /payment-methods` conforme a arquitetura.
- Implementar os clientes HTTP do BFF por interfaces próprias, com limite de dois segundos por consulta, sem retry automático nem fallback. Distinguir produto inexistente de falhas, timeout e resposta inválida da dependência.
- Usar servidor HTTP controlado nos testes para reproduzir sucesso, indisponibilidade, timeout e resposta inválida, sem adicionar endpoints de falha ao simulador entregue.

### 3. Carrinho e pré-checkout

- Validar a entrada de acordo com a spec, incluindo lista vazia, slugs inválidos ou repetidos e produtos inexistentes.
- Consultar produtos e meios por suas interfaces e só criar o carrinho quando todas as consultas necessárias forem bem-sucedidas. Registrar no Redis compartilhado o estado `PENDENTE` e o snapshot de produtos, preços e meios retornados.
- Retornar o contrato estável do pré-checkout e os erros padronizados, incluindo indisponibilidade do Redis. Não armazenar dados de cartão no carrinho.

### 4. Checkout e concorrência

- Validar carrinho, meio permitido e dados exigidos para cartão ou Pix. Calcular o total usando representação decimal exata e os preços associados ao carrinho.
- Simular recusa para cartão terminado em `0002`, mantendo o carrinho pendente; simular aprovação para os demais cartões válidos e Pix, produzindo os dados previstos na spec.
- Usar operação atômica Lua no Redis para verificar e aprovar o carrinho sem permitir duas aprovações concorrentes, inclusive a partir de instâncias diferentes. Rejeitar novas tentativas para carrinho aprovado.
- Se a resposta da operação Redis se perder e o resultado ficar desconhecido, retornar `424` com aviso de incerteza. Uma nova tentativa verifica o estado e não reaplica uma aprovação confirmada.
- Nunca persistir nem retornar número completo de cartão ou CVV. Produzir logs sem corpos de requisição/resposta nem dados de cartão, incluindo `cartId` quando disponível.

### 5. Contratos e verificação

- Implementar os contratos de sucesso, a tabela de HTTP/códigos e as mensagens, tipos de exibição e botões da spec. O caminho `/checkout/produtos` é uma convenção para o frontend, cuja implementação não está no escopo do BFF.
- Cobrir pré-checkout, sucessos de cartão e Pix, formatos e códigos de erro, validações, produto/carrinho inexistente, meio inválido, recusa seguida de nova tentativa e duplicidade.
- Usar Redis real isolado em contêiner para verificar a operação atômica e chamadas concorrentes entre instâncias; testar falha de dependências e usar proxy controlado para reproduzir resultado desconhecido com operação aplicada e não aplicada.
- Executar a suíte de testes e as verificações do build que forem implementadas; registrar comandos, resultados e quaisquer limites. O README deve explicar como iniciar, simular sucesso/recusa e a perda dos carrinhos se o Redis reiniciar sem persistência.

### 6. Avaliação independente

- Após a implementação e seus testes, executar `$assessment-spec` em uma etapa independente. A avaliação deve apresentar evidências para cada critério obrigatório, resultados dos testes e lacunas não verificadas.
- Corrigir divergências identificadas quando solicitado e repetir as verificações afetadas antes de nova avaliação.

## Condições de aceite

O trabalho está pronto para avaliação quando todos os contratos e regras obrigatórios da spec estiverem implementados; a prevenção de duplicidade for demonstrada com instâncias distintas e Redis compartilhado; os testes previstos tiverem resultado conhecido; e as limitações estiverem documentadas. Diferenciais não escolhidos continuam opcionais.
