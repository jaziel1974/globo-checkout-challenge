package br.com.globo.checkout.bff.api;

import static org.junit.jupiter.api.Assertions.*;

import br.com.globo.checkout.bff.CheckoutBffApplication;
import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.bff.domain.Ports;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** HTTP-level v2 contract tests against real Kafka and Redis without starting the worker listener. */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(classes = CheckoutBffApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"spring.kafka.admin.auto-create=false", "spring.data.redis.timeout=500ms", "spring.data.redis.connect-timeout=500ms"})
class CheckoutV2ApiIntegrationTest {
    private static final String COMMAND_TOPIC = "checkout-v2-commands";
    private static final String GROUP = "checkout-v2-api-test-" + UUID.randomUUID();
    private static final BigDecimal PRICE = new BigDecimal("10.10");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Container static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"))
        .withEnv("KAFKA_OPTS", "-XX:-UseBASE64Intrinsics");
    @Container static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379);

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("checkout.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @BeforeAll static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(COMMAND_TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired Ports.CartRepository carts;
    @Autowired PaymentPorts.AttemptStore attempts;
    @Autowired ObjectMapper mapper;
    @Autowired DefaultKafkaProducerFactory<String, String> v2ProducerFactory;
    @LocalServerPort int port;

    @Test void v2UsesRealKafkaReturnsV1SuccessShapeAndRejectsConcurrentAttemptWithP1005() throws Exception {
        Map<String, Object> kafkaProperties = v2ProducerFactory.getConfigurationProperties();
        assertEquals("all", kafkaProperties.get("acks"));
        assertEquals(true, kafkaProperties.get("enable.idempotence"));
        assertEquals(0, kafkaProperties.get("linger.ms"));
        assertEquals(1500, kafkaProperties.get("request.timeout.ms"));
        assertEquals(2000, kafkaProperties.get("delivery.timeout.ms"));
        assertEquals(2000, kafkaProperties.get("max.block.ms"));
        String cartId = createCart();
        String body = request(cartId, PaymentCommand.CREDIT, "4111111111111234", "7");
        try (KafkaConsumer<String, String> consumer = commandConsumer()) {
            CompletableFuture<HttpResponse<String>> first = postAsync("/v2/operations/checkout", body, "application/json");
            ConsumerRecord<String, String> record = awaitRecord(consumer);
            JsonNode wire = mapper.readTree(record.value());
            assertEquals(cartId, wire.get("cartId").asText());
            assertEquals("1234", wire.get("lastDigits").asText());
            assertFalse(record.value().contains("4111111111111234"));
            assertFalse(record.value().contains("\"cvv\""));
            assertEquals(5, wire.size());

            HttpResponse<String> duplicate = post("/v2/operations/checkout", body, "application/json");
            assertEquals(409, duplicate.statusCode());
            JsonNode duplicateJson = mapper.readTree(duplicate.body());
            assertEquals("PAYMENT_IN_PROGRESS", duplicateJson.get("errorCode").asText());
            assertEquals("P-1005", duplicateJson.path("frontendError").path("code").asText());
            assertFalse(duplicateJson.path("frontendError").has("secondaryButton"));

            completeFromKafka(record);
            HttpResponse<String> approved = first.get(5, TimeUnit.SECONDS);
            assertEquals(200, approved.statusCode());
            JsonNode json = mapper.readTree(approved.body());
            assertEquals("APROVADO", json.path("status").asText());
            assertEquals("1234", json.path("payment").path("lastDigits").asText());
            assertEquals(0, PRICE.compareTo(json.path("payment").path("amount").decimalValue()));
            assertTrue(json.path("paidAt").isTextual());

            String v1CartId = createCart();
            HttpResponse<String> v1 = post("/operations/checkout",
                request(v1CartId, PaymentCommand.CREDIT, "4111111111111234", "7"), "application/json");
            assertEquals(200, v1.statusCode());
            JsonNode v1Json = mapper.readTree(v1.body());
            assertEquals(v1Json.path("payment").path("amount").toString(), json.path("payment").path("amount").toString(),
                "v2 amount JSON representation must match v1");
            assertEquals(fieldNames(v1Json), fieldNames(json));
            assertEquals(fieldNames(v1Json.path("payment")), fieldNames(json.path("payment")));
            assertEquals(v1Json.path("status").asText(), json.path("status").asText());
            assertEquals(v1Json.path("payment").path("amount").decimalValue(), json.path("payment").path("amount").decimalValue());

            HttpResponse<String> alreadyApprovedBeforeMethodValidation = post("/v2/operations/checkout",
                "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"unknown\"}}", "application/json");
            assertEquals(409, alreadyApprovedBeforeMethodValidation.statusCode());
            assertEquals("PAYMENT_ALREADY_APPROVED", mapper.readTree(alreadyApprovedBeforeMethodValidation.body()).get("errorCode").asText());
        }
    }

    @Test void pixWireOmitsCardDigitsAndResponseMatchesV1PixShape() throws Exception {
        String cartId = createCart();
        try (KafkaConsumer<String, String> consumer = commandConsumer()) {
            CompletableFuture<HttpResponse<String>> v2Future = postAsync("/v2/operations/checkout",
                request(cartId, PaymentCommand.PIX, null, null), "application/json");
            ConsumerRecord<String, String> record = awaitRecord(consumer);
            JsonNode wire = mapper.readTree(record.value());
            assertFalse(wire.has("lastDigits"));
            assertEquals(4, wire.size());
            completeFromKafka(record);
            HttpResponse<String> v2 = v2Future.get(5, TimeUnit.SECONDS);
            assertEquals(200, v2.statusCode());
            JsonNode v2Json = mapper.readTree(v2.body());
            assertTrue(v2Json.path("payment").has("pixCode"));
            assertFalse(v2Json.path("payment").has("lastDigits"));

            String v1CartId = createCart();
            HttpResponse<String> v1 = post("/operations/checkout", request(v1CartId, PaymentCommand.PIX, null, null), "application/json");
            assertEquals(200, v1.statusCode());
            JsonNode v1Json = mapper.readTree(v1.body());
            assertEquals(fieldNames(v1Json), fieldNames(v2Json));
            assertEquals(fieldNames(v1Json.path("payment")), fieldNames(v2Json.path("payment")));
            assertEquals(v1Json.path("payment").path("amount").decimalValue(), v2Json.path("payment").path("amount").decimalValue());
        }
    }

    @Test void fourDigitCardAndOneDigitCvvAreAcceptedAndRefusalAllowsNewAttempt() throws Exception {
        String cartId = createCart();
        try (KafkaConsumer<String, String> consumer = commandConsumer()) {
            CompletableFuture<HttpResponse<String>> declinedFuture = postAsync("/v2/operations/checkout",
                request(cartId, PaymentCommand.CREDIT, "0002", "7"), "application/json");
            ConsumerRecord<String, String> declinedRecord = awaitRecord(consumer);
            completeFromKafka(declinedRecord);
            HttpResponse<String> declined = declinedFuture.get(5, TimeUnit.SECONDS);
            assertEquals(422, declined.statusCode());
            assertEquals("PAYMENT_DECLINED", mapper.readTree(declined.body()).get("errorCode").asText());
            HttpResponse<String> v1Declined = post("/operations/checkout",
                request(cartId, PaymentCommand.CREDIT, "0002", "7"), "application/json");
            assertEquals(422, v1Declined.statusCode());
            assertEquals(mapper.readTree(v1Declined.body()), mapper.readTree(declined.body()));

            CompletableFuture<HttpResponse<String>> approvedFuture = postAsync("/v2/operations/checkout",
                request(cartId, PaymentCommand.CREDIT, "1234", "7"), "application/json");
            ConsumerRecord<String, String> approvedRecord = awaitRecord(consumer);
            completeFromKafka(approvedRecord);
            HttpResponse<String> approved = approvedFuture.get(5, TimeUnit.SECONDS);
            assertEquals(200, approved.statusCode(), "retry after a confirmed refusal should be accepted: " + approved.body());
        }
    }

    @Test void lateResultAfterHttpDeadlineIsNotCancelledAndApprovedMarkerWinsBeforeCardValidation() throws Exception {
        String cartId = createCart();
        try (KafkaConsumer<String, String> consumer = commandConsumer()) {
            long started = System.nanoTime();
            CompletableFuture<HttpResponse<String>> waiting = postAsync("/v2/operations/checkout",
                request(cartId, PaymentCommand.CREDIT, "4111", "7"), "application/json");
            ConsumerRecord<String, String> record = awaitRecord(consumer);
            HttpResponse<String> unknown = waiting.get(7, TimeUnit.SECONDS);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals(424, unknown.statusCode());
            assertEquals("DEPENDENCY_UNAVAILABLE", mapper.readTree(unknown.body()).get("errorCode").asText());
            assertTrue(elapsedMillis >= 4_500 && elapsedMillis <= 6_500,
                "five-second global deadline with documented HTTP scheduling/transmission tolerance; elapsed=" + elapsedMillis);

            // The HTTP timeout does not cancel an already published command or its Redis reservation.
            completeFromKafka(record);
            HttpResponse<String> retryWithInvalidMethod = post("/v2/operations/checkout",
                "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"unknown\"}}", "application/json");
            assertEquals(409, retryWithInvalidMethod.statusCode());
            assertEquals("PAYMENT_ALREADY_APPROVED", mapper.readTree(retryWithInvalidMethod.body()).get("errorCode").asText());
        }
    }

    @Test void interventionBlocksNewHttpAttemptAndResponsePollingObservesRegisteredResultPromptly() throws Exception {
        String blockedCart = createCart();
        PaymentCommand blockedCommand = commandFor(blockedCart, "4242");
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(blockedCommand));
        assertTrue(attempts.markIntervention(blockedCommand.attemptId(), "OPERATOR_REVIEW"));
        HttpResponse<String> intervention = post("/v2/operations/checkout",
            request(blockedCart, PaymentCommand.CREDIT, "12", "7"), "application/json");
        assertEquals(409, intervention.statusCode());
        assertEquals("PAYMENT_IN_PROGRESS", mapper.readTree(intervention.body()).get("errorCode").asText());

        String cartId = createCart();
        try (KafkaConsumer<String, String> consumer = commandConsumer()) {
            CompletableFuture<HttpResponse<String>> response = postAsync("/v2/operations/checkout",
                request(cartId, PaymentCommand.CREDIT, "4242", "7"), "application/json");
            ConsumerRecord<String, String> record = awaitRecord(consumer);
            Thread.sleep(250);
            long completedAt = System.nanoTime();
            completeFromKafka(record);
            HttpResponse<String> result = response.get(2, TimeUnit.SECONDS);
            long observedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - completedAt);
            assertEquals(200, result.statusCode());
            assertTrue(observedMillis < 700,
                "100ms Redis polling should observe a recorded result promptly; tolerance includes scheduling, elapsed=" + observedMillis);
        }
    }

    @Test void lostRedisCreateResponseAfterRealMutationReturnsUnknownAndLeavesReservationRecoverable() {
        String cartId = createCart();
        PaymentPorts.AttemptStore store = Mockito.mock(PaymentPorts.AttemptStore.class, AdditionalAnswers.delegatesTo(attempts));
        Mockito.doAnswer(invocation -> {
            PaymentCommand command = invocation.getArgument(0);
            assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(command));
            throw new IllegalStateException("simulated lost Redis response after Lua mutation");
        }).when(store).create(Mockito.any());
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var service = new br.com.globo.checkout.bff.application.CheckoutV2Service(carts, store,
                new br.com.globo.checkout.worker.application.PublicationCoordinator(store,
                    (command, timeout) -> PaymentPorts.PublishResult.KNOWN_FAILURE), executor);
            var error = assertThrows(br.com.globo.checkout.bff.domain.CheckoutError.class, () -> service.checkout(
                new Requests.CheckoutRequest(cartId, new Requests.PaymentSelection(PaymentCommand.PIX, null)),
                System.nanoTime() + Duration.ofSeconds(2).toNanos()));
            assertEquals(br.com.globo.checkout.bff.domain.CheckoutError.Kind.UNKNOWN_APPROVAL, error.kind());
            assertEquals("AGUARDANDO_PUBLICACAO", attempts.findActiveByCart(cartId).orElseThrow().state());
            assertEquals(0, attempts.findActiveByCart(cartId).orElseThrow().publicationCount());
        } finally { executor.shutdownNow(); }
    }

    @Test void invalidJsonNonStringCardUnsupportedMediaAndNonUuidMissingCartKeepExistingErrorContract() throws Exception {
        HttpResponse<String> malformed = post("/v2/operations/checkout", "{", "application/json");
        HttpResponse<String> v1Malformed = post("/operations/checkout", "{", "application/json");
        assertEquals(400, malformed.statusCode());
        assertEquals("INVALID_REQUEST", mapper.readTree(malformed.body()).get("errorCode").asText());
        assertEquals(mapper.readTree(v1Malformed.body()), mapper.readTree(malformed.body()));

        String cartId = createCart();
        HttpResponse<String> numberObject = post("/v2/operations/checkout",
            "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"Cartão de Crédito\",\"card\":{\"holderName\":\"A\",\"number\":1234,\"expiry\":\"12/28\",\"cvv\":\"7\"}}}",
            "application/json");
        HttpResponse<String> v1NumberObject = post("/operations/checkout",
            "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"Cartão de Crédito\",\"card\":{\"holderName\":\"A\",\"number\":1234,\"expiry\":\"12/28\",\"cvv\":\"7\"}}}",
            "application/json");
        assertEquals(400, numberObject.statusCode());
        assertEquals("INVALID_REQUEST", mapper.readTree(numberObject.body()).get("errorCode").asText());
        assertEquals(mapper.readTree(v1NumberObject.body()), mapper.readTree(numberObject.body()));

        String unsupportedMethod = "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"Outro\"}}";
        HttpResponse<String> v2Method = post("/v2/operations/checkout", unsupportedMethod, "application/json");
        HttpResponse<String> v1Method = post("/operations/checkout", unsupportedMethod, "application/json");
        assertEquals(400, v2Method.statusCode());
        assertEquals(mapper.readTree(v1Method.body()), mapper.readTree(v2Method.body()));

        HttpResponse<String> unsupported = post("/v2/operations/checkout", "{}", "text/plain");
        HttpResponse<String> v1Unsupported = post("/operations/checkout", "{}", "text/plain");
        assertEquals(v1Unsupported.statusCode(), unsupported.statusCode());
        assertEquals(mapper.readTree(v1Unsupported.body()), mapper.readTree(unsupported.body()));

        HttpResponse<String> missing = post("/v2/operations/checkout",
            request("not-a-uuid", PaymentCommand.PIX, null, null), "application/json");
        assertEquals(404, missing.statusCode());
        assertEquals("CART_NOT_FOUND", mapper.readTree(missing.body()).get("errorCode").asText());
    }

    private void completeFromKafka(ConsumerRecord<String, String> record) throws Exception {
        PaymentCommand command = mapper.readValue(record.value(), PaymentCommand.class);
        var snapshot = attempts.find(command.attemptId()).orElseThrow();
        var lease = attempts.acquire(command.attemptId()).orElseThrow();
        assertEquals(1, attempts.reserveExecution(lease).orElseThrow());
        boolean declined = "0002".equals(command.lastDigits());
        var result = new PaymentResult(command.cartId(), declined ? PaymentResult.Outcome.DECLINED : PaymentResult.Outcome.APPROVED,
            declined ? null : Instant.now(), command.paymentMethod(), command.lastDigits(),
            PaymentCommand.PIX.equals(command.paymentMethod()) ? "PIX-SIMULADO-" + command.cartId() : null,
            PRICE, "BRL", List.of(new PaymentResult.ProductResult("basic", "Plano Básico", PRICE)),
            declined ? "PAYMENT_DECLINED" : null);
        assertEquals(PaymentPorts.CompletionResult.COMPLETED, attempts.complete(lease, result));
    }

    private String createCart() {
        String cartId = UUID.randomUUID().toString();
        carts.create(new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("basic", "Plano Básico", "Assinatura", PRICE, "BRL")),
            List.of(new Models.PaymentMethod(PaymentCommand.CREDIT), new Models.PaymentMethod(PaymentCommand.PIX))));
        return cartId;
    }

    private PaymentCommand commandFor(String cartId, String lastDigits) {
        return new PaymentCommand(PaymentCommand.CURRENT_SCHEMA_VERSION, UUID.randomUUID().toString(), cartId,
            PaymentCommand.CREDIT, lastDigits);
    }

    private KafkaConsumer<String, String> commandConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        var partition = new TopicPartition(COMMAND_TOPIC, 0);
        consumer.assign(List.of(partition));
        consumer.seekToEnd(List.of(partition));
        consumer.poll(Duration.ofMillis(100));
        return consumer;
    }

    private ConsumerRecord<String, String> awaitRecord(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) return records.iterator().next();
        }
        fail("No command record arrived in Kafka");
        return null;
    }

    private CompletableFuture<HttpResponse<String>> postAsync(String path, String body, String contentType) {
        return CompletableFuture.supplyAsync(() -> post(path, body, contentType));
    }

    private HttpResponse<String> post(String path, String body, String contentType) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(7)).header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
            return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception ex) { throw new RuntimeException(ex); }
    }

    private static String request(String cartId, String method, String number, String cvv) {
        if (PaymentCommand.PIX.equals(method)) return "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"Pix\"}}";
        return "{\"cartId\":\"" + cartId + "\",\"paymentMethod\":{\"name\":\"" + method
            + "\",\"card\":{\"holderName\":\"A\",\"number\":\"" + number
            + "\",\"expiry\":\"12/28\",\"cvv\":\"" + cvv + "\"}}}";
    }

    private static java.util.Set<String> fieldNames(JsonNode node) {
        java.util.Set<String> names = new java.util.TreeSet<>();
        node.propertyNames().forEach(names::add);
        return names;
    }
}
