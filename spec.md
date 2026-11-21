# Especificação - BFF de checkout de assinaturas

## Objetivo e contexto

Construir um Backend for Frontend (BFF) para um checkout de assinatura com três telas: pagamento, sucesso e erro. O fluxo tem duas etapas: pré-checkout, que monta o carrinho, e checkout, que simula a cobrança e pode terminar em sucesso ou erro.

O BFF não é a fonte dos produtos nem dos meios de pagamento. Ele consulta serviços externos, que podem ser simulados no próprio projeto, e retorna ao frontend apenas os dados necessários para cada tela. Os serviços devem ser consumidos por interfaces próprias, sem leitura direta dos dados no endpoint.

## Pré-checkout

### `POST /operations/pre-checkout`

Recebe os produtos escolhidos, identificados por slug:

```json
{
  "products": [{ "slug": "premium" }]
}
```

O BFF consulta o serviço de produtos e o serviço de meios de pagamento e retorna um carrinho identificado por `cartId`, pronto para a tela de pagamento. Exemplo fornecido:

```json
{
  "cartId": "uuid",
  "products": [
    {
      "slug": "premium",
      "name": "Plano Premium",
      "description": "Assinatura mensal com catálogo completo",
      "price": 39.90,
      "currency": "BRL"
    }
  ],
  "paymentMethods": [
    { "name": "Cartão de Crédito" },
    { "name": "Cartão de Débito" },
    { "name": "Pix" }
  ]
}
```

O catálogo fixo sugerido para o serviço de produtos é:

| Plano | Preço mensal |
| --- | ---: |
| Básico | R$ 19,90 |
| Premium | R$ 39,90 |
| Família | R$ 59,90 |

Os três meios de pagamento devem vir do serviço de meios de pagamento, não de uma lista fixa no controller do BFF. Um produto inexistente deve gerar um erro no padrão descrito em [Contrato de erro](#contrato-de-erro).

## Checkout

### `POST /operations/checkout`

Recebe o identificador do carrinho e o meio de pagamento escolhido. Exemplo para cartão:

```json
{
  "cartId": "uuid",
  "paymentMethod": {
    "name": "Cartão de Crédito",
    "card": {
      "holderName": "Ana Souza",
      "number": "4111111111114242",
      "expiry": "12/28",
      "cvv": "123"
    }
  }
}
```

Para Pix, não enviar dados de cartão. A cobrança deve ser simulada, sem integração com gateway real.

### Sucesso

Retornar os dados do pagamento e dos produtos necessários à tela de sucesso. Exemplo para cartão:

```json
{
  "cartId": "uuid",
  "status": "APROVADO",
  "paidAt": "2026-10-14T16:00:00Z",
  "payment": {
    "method": "Cartão de Crédito",
    "lastDigits": "4242",
    "amount": 39.90,
    "currency": "BRL"
  },
  "products": [
    { "slug": "premium", "name": "Plano Premium", "price": 39.90 }
  ]
}
```

Para Pix aprovado, substituir `lastDigits` por um código Pix simulado. Não persistir o número completo do cartão nem o CVV; retornar somente os dados necessários à tela, como os últimos dígitos.

Exemplo de resposta Pix, usando o código simulado adotado nesta spec:

```json
{
  "cartId": "uuid",
  "status": "APROVADO",
  "paidAt": "2026-10-14T16:00:00Z",
  "payment": {
    "method": "Pix",
    "pixCode": "PIX-SIMULADO-uuid",
    "amount": 39.90,
    "currency": "BRL"
  },
  "products": [
    { "slug": "premium", "name": "Plano Premium", "price": 39.90 }
  ]
}
```

## Estabilidade dos contratos

As respostas de sucesso do pré-checkout e do checkout e as respostas de erro devem ter estrutura, nomes, tipos e significado dos campos previsíveis e documentados para o frontend. Os exemplos acima definem o formato adotado para as respostas de sucesso; as variações e decisões complementares estão descritas abaixo. Uma mudança incompatível nesses contratos exige atualização coordenada dos consumidores e da documentação.

No sucesso do pré-checkout, `cartId`, `products` e `paymentMethods` são obrigatórios; cada produto contém `slug`, `name`, `description`, `price` e `currency`, e cada meio contém `name`. No sucesso do checkout, `cartId`, `status`, `paidAt`, `payment` e `products` são obrigatórios; `payment` contém `method`, `amount` e `currency`. Para cartão, contém também `lastDigits`; para Pix, contém `pixCode`, com um código simulado, em lugar de `lastDigits`. Cada produto do checkout contém `slug`, `name` e `price`. Datas são strings no formato ISO 8601 com fuso UTC; preços e valores são números JSON em reais, e `currency` é `BRL`.

## Contrato de erro

Toda falha que o frontend precisa exibir deve usar o mesmo formato de objeto e uma resposta HTTP 4xx. Exemplo de pagamento recusado:

```json
{
  "displayType": "BLOCK",
  "errorCode": "PAYMENT_DECLINED",
  "cartId": "uuid",
  "frontendError": {
    "code": "P-1001",
    "title": "Pagamento não aprovado",
    "description": "Não foi possível concluir o pagamento. Tente novamente ou escolha outro meio.",
    "primaryButton": { "label": "Tentar novamente", "url": "RETRY" },
    "secondaryButton": {
      "label": "Trocar meio de pagamento",
      "url": "/checkout/pagamento"
    }
  }
}
```

- `displayType` pode ser `BLOCK`, para erro de tela com ações, ou `INTERVENTION`, para aviso que não encerra o fluxo.
- `frontendError` sempre contém `code`, `title`, `description` e pelo menos `primaryButton`.
- O formato do objeto de erro deve permanecer previsível para o frontend.
- `displayType`, `errorCode` e `frontendError` são obrigatórios em todos os erros; `cartId` está sempre presente e é `null` quando não há identificador de carrinho aplicável ou disponível. `secondaryButton` é opcional. Cada botão presente contém `label` e `url`.

## Regras de negócio

1. O checkout só pode ocorrer para um `cartId` criado no pré-checkout.
2. O meio escolhido deve ser um dos retornados no pré-checkout daquele carrinho.
3. Produto inexistente no pré-checkout deve retornar erro no padrão `frontendError`.
4. Cartão cujo número termina em `0002` deve ser recusado, permitindo demonstrar a tela de erro.
5. Um carrinho já aprovado não pode ser pago novamente.
6. Número completo do cartão e CVV não podem ser persistidos.

## Estrutura e entrega

- Separar o BFF, os clientes dos serviços externos e a regra de checkout.
- Tratar as falhas de negócio, incluindo produto inválido, carrinho inexistente, pagamento recusado e pagamento duplicado.
- Tornar a solução testável, cobrindo pré-checkout, sucesso e cenários de erro.
- Entregar o código em um repositório público, com instruções de instalação e execução.
- Documentar no README como subir a API e simular sucesso e recusa.

## Diferenciais opcionais

- Spring Boot para estruturar a aplicação.
- Persistência com PostgreSQL ou MongoDB.
- Processamento assíncrono de pagamento com RabbitMQ ou Kafka.
- Cache de produtos e meios de pagamento com Redis.
- Testes automatizados com JUnit e Mockito.
- Execução com Docker.

## Decisões adotadas para pontos não detalhados

As escolhas desta seção foram feitas para esta solução; não são exigências adicionais atribuídas ao PDF. Elas tornam o comportamento verificável e devem ser refletidas na implementação e na documentação da API.

### Carrinho e tentativas de pagamento

O desafio estabelece que o carrinho é criado no pré-checkout, que o checkout usa um `cartId` criado nessa etapa e que um carrinho aprovado não pode ser pago novamente. A expiração, os demais estados e suas transições não são detalhados. Adotamos:

- Um carrinho começa em `PENDENTE` após o pré-checkout bem-sucedido. Após uma recusa, permanece `PENDENTE` e permite nova tentativa, inclusive com outro meio retornado para ele. Após aprovação, passa a `APROVADO` e novas tentativas recebem erro de pagamento duplicado. Isso permite demonstrar o fluxo de recusa e protege contra uma segunda cobrança aprovada.
- O carrinho não expira automaticamente nesta solução. Isso evita introduzir um prazo arbitrário; persistência durável continua sendo um diferencial opcional. Se for usado armazenamento em memória, os carrinhos deixam de existir ao reiniciar a aplicação, e essa limitação deve constar no README.
- Produtos, preços e meios retornados no pré-checkout são associados ao carrinho e usados no checkout. Assim, a tela de pagamento e a cobrança simulada usam os mesmos dados, mesmo que o serviço consultado mude depois.
- A verificação e a aprovação devem impedir aprovações duplicadas para o mesmo carrinho, inclusive em requisições concorrentes. A estratégia de sincronização depende do armazenamento escolhido; isso fecha a brecha entre verificar e atualizar o estado.

### Entradas, valores e simulação

- Para completar o catálogo simulado, adotamos `basico` / `Plano Básico` / `Assinatura mensal do plano Básico` e `familia` / `Plano Família` / `Assinatura mensal do plano Família` como `slug` / `name` / `description`. São textos ilustrativos, sem prometer benefícios não descritos no desafio; o exemplo de Premium permanece como fornecido.
- `products` deve ser uma lista não vazia de objetos com `slug` não vazio; slugs repetidos são rejeitados. Escolhemos rejeitar duplicatas porque o desafio não define quantidade nem a cobrança de um mesmo plano mais de uma vez. Slugs desconhecidos geram erro de produto inexistente.
- `cartId` e `paymentMethod.name` são obrigatórios. O nome do meio deve coincidir com um dos retornados para o carrinho. Para cartão, `holderName`, `number`, `expiry` e `cvv` são strings obrigatórias e não vazias; `number` contém apenas dígitos e pelo menos quatro deles, `cvv` contém apenas dígitos, e `expiry` segue `MM/YY`, com mês de `01` a `12`. Para Pix, `card` não deve ser enviado. Essas validações mínimas tornam a entrada previsível sem inventar regras de bandeira ou integração real.
- O valor cobrado é a soma dos preços dos produtos associados ao carrinho, em BRL. Os cálculos devem usar representação decimal exata, evitando erros de arredondamento; os exemplos de saída continuam usando números JSON em reais. Não há taxas ou descontos definidos.
- Na simulação, um cartão válido para as verificações acima e terminado em `0002` é recusado; os demais cartões são aprovados. Pix é aprovado e retorna `pixCode` simulado. Essas escolhas tornam os cenários reproduzíveis e não representam o processamento real de um pagamento Pix.
- O nome `pixCode` foi escolhido para tornar identificável o campo que o PDF deixa sem nome. Seu valor é a string `PIX-SIMULADO-` seguida do `cartId`; é apenas um identificador demonstrativo, não um código Pix utilizável. `lastDigits` contém apenas os quatro últimos dígitos do cartão. Número completo e CVV não devem ser persistidos nem retornados.

### Falhas dos serviços consultados

Cada consulta de produtos ou meios de pagamento tem limite de 2 segundos; timeout, indisponibilidade ou resposta inválida da dependência resultam em `DEPENDENCY_UNAVAILABLE`. Não há retry automático nem fallback para dados fixos ou antigos: a chamada pode ser repetida pelo usuário e o BFF não apresenta um carrinho baseado em dados incertos. O limite é uma escolha simples para a simulação, e não uma exigência do desafio. Os clientes simulados devem permitir testar essas falhas sem aguardar uma dependência real.

### Respostas HTTP e erros

Adotamos `200 OK` nos dois sucessos: ambos retornam imediatamente o resultado necessário à tela. Para falhas exibidas pelo frontend, escolhemos os códigos e identificadores abaixo para manter o contrato 4xx pedido no desafio e distinguir situações para o consumidor:

| Situação | HTTP | `errorCode` | `frontendError.code` |
| --- | --- | --- | --- |
| Entrada inválida, inclusive dados de cartão inválidos | 400 | `INVALID_REQUEST` | `V-1001` |
| Meio não permitido para o carrinho | 400 | `PAYMENT_METHOD_NOT_ALLOWED` | `P-1002` |
| Produto inexistente | 404 | `PRODUCT_NOT_FOUND` | `C-1001` |
| Carrinho inexistente | 404 | `CART_NOT_FOUND` | `C-1002` |
| Carrinho já aprovado | 409 | `PAYMENT_ALREADY_APPROVED` | `P-1003` |
| Pagamento recusado | 422 | `PAYMENT_DECLINED` | `P-1001` |
| Serviço consultado indisponível, com timeout ou resposta inválida; Redis indisponível | 424 | `DEPENDENCY_UNAVAILABLE` | `S-1001` |

Os códigos de `frontendError` acima, exceto o exemplo `P-1001` do desafio, são escolhas próprias para identificar cada situação de forma estável. O `424` é uma escolha para comunicar falha de dependência mantendo a exigência de 4xx para falhas exibidas; não implica que toda falha de infraestrutura seja causada pelo cliente. Nenhum carrinho é criado se uma consulta necessária ao pré-checkout falhar. Erros não devem expor dados de cartão, credenciais ou detalhes internos. Os textos e ações de `frontendError` de cada situação devem ser documentados de forma consistente, usando `BLOCK` ou `INTERVENTION` conforme a experiência escolhida para a tela.

Se a conexão com o Redis cair após o envio da operação de aprovação e seu resultado não puder ser confirmado, retornar HTTP `424` com `DEPENDENCY_UNAVAILABLE` e `S-1001`, informando que o resultado é incerto. Uma nova tentativa consulta o estado do carrinho e não reaplica uma aprovação confirmada. Esse erro não garante que a operação não ocorreu: ela pode ter sido concluída antes da perda de conexão. Essa regra foi escolhida durante a definição da arquitetura para manter a proteção contra duplicidade entre instâncias.

### Apresentação dos erros

As mensagens e ações abaixo foram escolhidas durante a definição da arquitetura. A rota `/checkout/produtos` é uma convenção proposta para reiniciar o fluxo no frontend; sua existência e implementação não fazem parte do BFF. `RETRY` representa uma ação do frontend de reenviar a operação após eventual correção dos dados, não uma URL HTTP. O frontend é responsável por interpretar essa ação.

| Situação | `displayType` | `title` | `description` | `primaryButton` (`label`, `url`) |
| --- | --- | --- | --- | --- |
| Entrada inválida | `INTERVENTION` | Dados inválidos | Confira os dados informados e tente novamente. | `Tentar novamente`, `RETRY` |
| Meio não permitido | `INTERVENTION` | Meio de pagamento indisponível | Escolha um dos meios de pagamento disponíveis para este carrinho. | `Tentar novamente`, `RETRY` |
| Produto inexistente | `BLOCK` | Produto não encontrado | Selecione os produtos novamente para iniciar o checkout. | `Escolher produtos`, `/checkout/produtos` |
| Carrinho inexistente | `BLOCK` | Carrinho não encontrado | Inicie um novo checkout para continuar. | `Escolher produtos`, `/checkout/produtos` |
| Carrinho já aprovado | `BLOCK` | Carrinho já pago | Este carrinho já foi aprovado. Inicie outro checkout para uma nova compra. | `Escolher produtos`, `/checkout/produtos` |
| Pagamento recusado | `BLOCK` | Pagamento não aprovado | Não foi possível concluir o pagamento. Tente novamente ou escolha outro meio. | `Tentar novamente`, `RETRY` |
| Dependência indisponível, com resultado conhecido | `INTERVENTION` | Serviço indisponível | Não foi possível concluir a operação. Tente novamente. | `Tentar novamente`, `RETRY` |
| Resultado de aprovação desconhecido | `INTERVENTION` | Resultado ainda não confirmado | O pagamento pode ter sido aprovado. Tente novamente para verificar o estado do carrinho. | `Verificar novamente`, `RETRY` |

Para pagamento recusado, incluir também `secondaryButton` com `label` igual a `Trocar meio de pagamento` e `url` igual a `/checkout/pagamento`, conforme o exemplo do desafio. Para os demais casos acima, omitir `secondaryButton`. A verificação após resultado desconhecido não cria uma segunda aprovação: se o estado já for `APROVADO`, retorna o erro de carrinho já pago.

### Verificação

Os testes devem cobrir os contratos de sucesso do pré-checkout, de cartão e de Pix; o formato comum e os códigos dos erros; entrada e produto inválidos; carrinho inexistente; meio não permitido; recusa e nova tentativa; pagamento duplicado, inclusive em concorrência; e indisponibilidade, timeout e resposta inválida das dependências. Isso verifica tanto as exigências do desafio quanto as decisões adotadas acima.

Fonte: `CKW-Desafio Backend-141026-171112 3 (1).pdf`.
