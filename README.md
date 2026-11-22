# Desafio Backend — Checkout de assinaturas

BFF em Java 21 e Spring Boot 4.1.2 para as telas de pagamento, sucesso e erro de um checkout de assinaturas. Produtos, meios de pagamento e cobrança são simulados. Não inclui frontend nem integração com gateway real.

## Desenvolvimento e evolução

A v1 foi implementada primeiro, com checkout síncrono. A v2 veio depois, na branch `feature/checkout-kafka`, simulando uma evolução com Kafka e consumidor independente. A implementação e os contratos da v1 foram preservados; a v2 tem rota própria.

O trabalho seguiu especificação, arquitetura, plano por etapas, implementação e revisão independente, com apoio de agentes de IA e validação a cada etapa. Decisões não definidas no desafio foram discutidas e documentadas.

> **Limite da validação humana:** por falta de tempo, a v2 não foi totalmente testada manualmente por uma pessoa. Os testes automatizados e as verificações conduzidas por agentes de IA não substituem uma validação humana completa.

## Executar

Pré-requisitos: JDK 21 (`JAVA_HOME` configurado) e Docker com Docker Compose. O Maven Wrapper está incluído. Os testes usam Redis e Kafka reais via Testcontainers; mantenha o Docker ativo.

Na raiz, compile e execute os testes:

```powershell
# Windows
./mvnw.cmd clean verify
```

```sh
# Linux/macOS
sh ./mvnw clean verify
```

Depois, inicie os serviços com os JARs gerados:

```sh
docker compose up --build -d
```

- BFF: `http://localhost:8080` — [Swagger UI](http://localhost:8080/swagger-ui.html) e [OpenAPI](http://localhost:8080/v3/api-docs).
- Simulador: `http://localhost:8081` — [Swagger UI](http://localhost:8081/swagger-ui.html).
- Para encerrar: `docker compose down`. O Redis não preserva os carrinhos após reiniciar.

## Organização

| Módulo | Responsabilidade |
| --- | --- |
| `checkout-bff` | API HTTP, pré-checkout, checkout v1 e entrada/espera da v2. |
| `payment-simulator` | Produtos e meios de pagamento em `GET /products/{slug}` e `GET /payment-methods`. |
| `checkout-v2-core` | Modelos, interfaces e adaptadores compartilhados da v2. |
| `checkout-worker` | Processo independente de consumo Kafka, pagamento simulado e recuperação de tentativas. |

O código separa API, aplicação, domínio e infraestrutura, com interfaces próprias para os serviços externos. Redis mantém carrinhos e tentativas com operações atômicas; Kafka transporta os comandos da v2.

## API e simulação

Envie os corpos abaixo como `application/json`. Os contratos completos de request, sucesso e erro estão na [spec](spec.md) e no [OpenAPI](docs/openapi.yaml).

| Método e rota | Comportamento |
| --- | --- |
| `POST /operations/pre-checkout` | Consulta produtos/meios e cria o carrinho. Retorna `cartId`, `products` e `paymentMethods`. |
| `POST /operations/checkout` | Checkout v1: simula o pagamento diretamente. |
| `POST /v2/operations/checkout` | Checkout v2: processa via Kafka e devolve o resultado no mesmo formato da v1. |

Crie um carrinho:

```json
{"products":[{"slug":"premium"}]}
```

O catálogo mensal adotado tem `basico` (R$ 19,90), `premium` (R$ 39,90) e `familia` (R$ 59,90). Os meios disponíveis são `Cartão de Crédito`, `Cartão de Débito` e `Pix`.

Use o `cartId` retornado no checkout da versão escolhida. Exemplo de cartão aprovado:

```json
{
  "cartId": "<cartId>",
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

Para simular recusa, use o mesmo corpo com `number` igual a `4111111111110002`: a resposta é HTTP `422 / PAYMENT_DECLINED`. Após a recusa, o carrinho permite outra tentativa. Para débito, troque `name` por `Cartão de Débito`.

Para Pix, não envie `card`:

```json
{"cartId":"<cartId>","paymentMethod":{"name":"Pix"}}
```

No sucesso (`200`), a resposta contém `cartId`, `status: APROVADO`, `paidAt`, pagamento e produtos. Cartão retorna `lastDigits`; Pix retorna `pixCode` simulado. O total em BRL é calculado a partir dos preços registrados no pré-checkout.

São validados o carrinho criado no pré-checkout e o meio disponível nele. Produto inexistente gera erro padronizado; carrinho aprovado não pode ser pago novamente. Número completo do cartão e CVV não são persistidos nem expostos em respostas, Kafka ou logs.

Os erros exibíveis usam HTTP 4xx e o envelope estável `displayType`, `errorCode`, `cartId` e `frontendError`. Este contém código, título, descrição e botão primário; o secundário é opcional. `displayType` admite `BLOCK` ou `INTERVENTION`.

| HTTP | Situações |
| --- | --- |
| `400` | Entrada inválida ou meio não permitido. |
| `404` | Produto ou carrinho inexistente. |
| `409` | Carrinho já aprovado; somente na v2, `PAYMENT_IN_PROGRESS` para tentativa em andamento ou aguardando intervenção. |
| `422` | Pagamento recusado. |
| `424` | Dependência indisponível ou resultado ainda não confirmado. |

## Particularidades e limites da v2

A v2 recebe o mesmo JSON da v1, sem cabeçalho adicional de idempotência. Aguarda até cinco segundos no total, incluindo até dois segundos para confirmar a publicação Kafka. Retorna o resultado diretamente, sem `202` ou endpoint de consulta.

Um `424` por resultado incerto não cancela o pagamento. Outra chamada pode receber `409` por processamento/aprovação ou iniciar nova tentativa após recusa. O resultado anterior pode não ser recuperável. Recuperação, limites de tentativas e intervenção estão no [guia de operação Kafka](docs/operacao-kafka.md).

Use carrinhos distintos por versão: não há proteção entre v1 e v2. Redis não tem persistência nem expiração automática; sua perda elimina também a deduplicação. Kafka local tem um broker, sem redundância, e retenção de sete dias nos tópicos de comandos e falhas.

## Testes e documentação

Em 22/11/2026, a última validação registrada passou com **107 testes automatizados**, sem falhas ou testes ignorados (JUnit, Mockito e Testcontainers). Cobrem pré-checkout, sucesso, erros, dependências e concorrência. Agentes de IA também verificaram o Compose com cartão, Pix, recusa, dois BFFs concorrentes, indisponibilidades e recuperação.

Foram adotados os diferenciais Spring Boot, Kafka, JUnit/Mockito e Docker. PostgreSQL/MongoDB e cache de consultas de produtos/meios não foram implementados; Redis armazena carrinhos e tentativas.

| Documentação | Conteúdo |
| --- | --- |
| [Enunciado](<CKW-Desafio Backend-141026-171112 3 (1).pdf>) | Documento original do desafio. |
| [Spec v1](spec.md) · [Spec Kafka](spec_kafka.md) | Requisitos, contratos, erros e decisões adotadas. |
| [Arquitetura v1](arquitetura.md) · [Arquitetura Kafka](arquitetura-kafka.md) | Componentes, interfaces e coordenação. |
| [Plano v1](plano.md) · [Plano Kafka](plano-kafka.md) | Etapas de desenvolvimento e validação. |
| [Operação Kafka](docs/operacao-kafka.md) | Tentativas, fila de falhas, inspeção e reprocessamento manual. |
| [Validação integrada](validacao-kafka-iteracao-8.md) · [Regressão](validacao-kafka-iteracao-9.md) · [Avaliação independente](avaliacao-kafka-final.md) | Evidências e limites das verificações locais. |
