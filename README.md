# BFF de checkout

Implementação em Java 21 e Spring Boot 4.1.2 com dois módulos Maven:

- `checkout-bff`: endpoints de pré-checkout e checkout; usa Redis para guardar o snapshot do carrinho e aprová-lo de forma atômica.
- `payment-simulator`: serviço HTTP de produtos e meios de pagamento.

## Pré-requisitos

- Java 21
- Docker com Docker Compose
- Maven Wrapper (`mvnw.cmd` no Windows ou `sh ./mvnw` em Linux/macOS; baixa o Maven 3.9.9 na primeira execução)

## Build e testes

```powershell
./mvnw.cmd clean verify
```

Os testes unitários e de cliente HTTP não precisam de dependências externas. O teste de integração do Redis usa Testcontainers e requer que o Docker esteja ativo.

## Executar

```powershell
./mvnw.cmd clean package
docker compose up --build
```

O BFF fica em `http://localhost:8080`; o simulador em `http://localhost:8081`. O Compose também inicia Redis sem persistência. É possível configurar `SERVER_PORT`, `REDIS_HOST`, `REDIS_PORT`, `PRODUCTS_BASE_URL` e `PAYMENT_METHODS_BASE_URL` por ambiente. As URLs padrão para execução local apontam ao simulador em `localhost:8081`.

## Documentação Swagger/OpenAPI

- Swagger UI do BFF: `http://localhost:8080/swagger-ui.html`
- Especificação OpenAPI do BFF: `http://localhost:8080/v3/api-docs`
- Swagger UI do simulador: `http://localhost:8081/swagger-ui.html`
- Especificação OpenAPI do simulador: `http://localhost:8081/v3/api-docs`

## Simular os fluxos

Crie o carrinho:

```http
POST http://localhost:8080/operations/pre-checkout
Content-Type: application/json

{"products":[{"slug":"premium"}]}
```

Use o `cartId` da resposta em `POST /operations/checkout`. Pix não recebe objeto `card`:

```json
{"cartId":"<cartId>","paymentMethod":{"name":"Pix"}}
```

Cartão aprovado:

```json
{"cartId":"<cartId>","paymentMethod":{"name":"Cartão de Crédito","card":{"holderName":"Ana Souza","number":"4111111111114242","expiry":"12/28","cvv":"123"}}}
```

Para demonstrar recusa, use um número que termine em `0002`, por exemplo `4111111111110002`. A recusa mantém o carrinho pendente para nova tentativa. Depois de aprovação, nova tentativa com o carrinho retorna `PAYMENT_ALREADY_APPROVED`.

O simulador oferece os slugs `basico`, `premium` e `familia`; os meios disponíveis são obtidos por HTTP do simulador e gravados no snapshot do carrinho.

## Estado e segurança

Os carrinhos não expiram automaticamente e são compartilhados entre instâncias do BFF pelo Redis. O Redis do Compose não tem persistência; se ele reiniciar ou perder os dados, os carrinhos existentes são perdidos e seu uso retorna `CART_NOT_FOUND`. Reiniciar somente o BFF não remove carrinhos enquanto Redis continuar disponível.

O checkout é uma simulação, sem gateway. O número completo e o CVV do cartão não são persistidos nem retornados. Os logs registram eventos e `cartId`, sem corpos de requisição/resposta nem dados de cartão.
