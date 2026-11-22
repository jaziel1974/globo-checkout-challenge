package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;

import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.worker.CheckoutWorkerApplication;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.config.ScheduledTaskHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Verifies the operational CLI profile against real Redis/Kafka without starting background consumption. */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(classes = CheckoutWorkerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {"spring.kafka.admin.auto-create=false", "spring.data.redis.timeout=500ms", "spring.data.redis.connect-timeout=500ms"})
@ActiveProfiles("worker-cli")
class WorkerCliRealIntegrationTest {
    @Container static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"))
        .withEnv("KAFKA_OPTS", "-XX:-UseBASE64Intrinsics");
    @Container static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379);

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @org.junit.jupiter.api.BeforeAll static void topics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("checkout-v2-commands", 1, (short) 1),
                new NewTopic("checkout-v2-failures", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired WorkerCli cli;
    @Autowired PaymentPorts.AttemptStore attempts;
    @Autowired StringRedisTemplate redisTemplate;
    @Autowired KafkaListenerEndpointRegistry listeners;
    @Autowired ScheduledTaskHolder scheduledTasks;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;

    @Test void authorizingProcessingBeforePublicationBackoffIsSuccessfulAndLeavesSchedulerToResumeIt() throws Exception {
        assertTrue(listeners.getListenerContainers().isEmpty(), "CLI profile must not start Kafka listeners");
        assertTrue(scheduledTasks.getScheduledTasks().isEmpty(), "CLI profile must not start a recovery scheduler");
        try (var connection = redisTemplate.getConnectionFactory().getConnection()) { connection.serverCommands().flushDb(); }

        for (int publicationCount : List.of(1, 2)) {
            String cartId = UUID.randomUUID().toString();
            String attemptId = UUID.randomUUID().toString();
            persistV1Cart(cartId);
            PaymentCommand command = new PaymentCommand(1, attemptId, cartId, PaymentCommand.PIX, null);
            assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(command));
            assertEquals(PaymentPorts.PublishReservation.RESERVED, attempts.reservePublication(attemptId));
            if (publicationCount == 2) {
                String key = "checkout:v2:attempt:" + attemptId;
                redisTemplate.opsForHash().put(key, "nextPublicationAt", "0");
                redisTemplate.opsForZSet().add("checkout:v2:publication-due", attemptId, 0);
                assertEquals(PaymentPorts.PublishReservation.RESERVED, attempts.reservePublication(attemptId));
            }
            assertEquals(publicationCount, attempts.find(attemptId).orElseThrow().publicationCount());
            assertTrue(attempts.markIntervention(attemptId, "OPERATOR_REVIEW"));
            long authorizedAt = System.currentTimeMillis();

            assertEquals(0, cli.execute(new String[]{"authorize-processing", "--attempt-id", attemptId}));
            var after = attempts.find(attemptId).orElseThrow();
            assertEquals("AGUARDANDO_PUBLICACAO", after.state());
            assertEquals(publicationCount, after.publicationCount(), "authorization must not reset or consume publication budget");
            assertEquals(0, after.executionCount());
            assertTrue(after.nextPublicationAtMillis() > authorizedAt,
                "authorization must preserve the existing 30-second publication backoff");
        }
    }

    @Test void realCliCommandsValidateExactDltOffsetAndRejectTerminalOrMissingAttempts() throws Exception {
        assertTrue(listeners.getListenerContainers().isEmpty());
        assertTrue(scheduledTasks.getScheduledTasks().isEmpty());
        try (var connection = redisTemplate.getConnectionFactory().getConnection()) { connection.serverCommands().flushDb(); }

        String cartId = UUID.randomUUID().toString();
        String attemptId = UUID.randomUUID().toString();
        persistV1Cart(cartId);
        PaymentCommand command = new PaymentCommand(1, attemptId, cartId, PaymentCommand.PIX, null);
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(command));
        assertTrue(attempts.markIntervention(attemptId, "REDIS_UNAVAILABLE"));

        kafkaTemplate.send("checkout-v2-failures", "prior", "{\"schemaVersion\":1}")
            .get(10, TimeUnit.SECONDS);
        long sourceOffset = 10_000;
        String dltJson = "{\"schemaVersion\":1,\"attemptId\":\"" + attemptId + "\",\"cartId\":\"" + cartId
            + "\",\"sourceTopic\":\"checkout-v2-commands\",\"sourcePartition\":0,\"sourceOffset\":"
            + sourceOffset + ",\"reasonCode\":\"REDIS_UNAVAILABLE\"}";
        long dltOffset = kafkaTemplate.send("checkout-v2-failures", attemptId, dltJson)
            .get(10, TimeUnit.SECONDS).getRecordMetadata().offset();
        assertNotEquals(sourceOffset, dltOffset, "the DLT position is distinct from the original command position");

        assertEquals(0, cli.execute(new String[]{"inspect", "--attempt-id", attemptId}));
        assertEquals(0, cli.execute(new String[]{"mark-intervention", "--attempt-id", attemptId,
            "--failure-partition", "0", "--failure-offset", Long.toString(dltOffset)}));
        assertEquals(0, cli.execute(new String[]{"authorize-processing", "--attempt-id", attemptId}));
        assertEquals("PUBLICADO", attempts.find(attemptId).orElseThrow().state(),
            "authorized processing publishes and confirms the same attempt when its backoff is already due");

        String exhaustedCart = UUID.randomUUID().toString();
        String exhaustedAttempt = UUID.randomUUID().toString();
        persistV1Cart(exhaustedCart);
        PaymentCommand exhausted = new PaymentCommand(1, exhaustedAttempt, exhaustedCart, PaymentCommand.PIX, null);
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(exhausted));
        redisTemplate.opsForHash().put("checkout:v2:attempt:" + exhaustedAttempt, "publicationCount", "3");
        assertTrue(attempts.markIntervention(exhaustedAttempt, "PUBLICATION_EXHAUSTED"));
        assertEquals(0, cli.execute(new String[]{"authorize-publication", "--attempt-id", exhaustedAttempt}));
        assertTrue(attempts.find(exhaustedAttempt).orElseThrow().publicationCount() > 0);

        String terminalCart = UUID.randomUUID().toString();
        String terminalAttempt = UUID.randomUUID().toString();
        persistV1Cart(terminalCart);
        PaymentCommand terminal = new PaymentCommand(1, terminalAttempt, terminalCart, PaymentCommand.PIX, null);
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(terminal));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, attempts.reservePublication(terminalAttempt));
        assertTrue(attempts.confirmPublication(terminalAttempt));
        var lease = attempts.acquire(terminalAttempt).orElseThrow();
        assertTrue(attempts.reserveExecution(lease).isPresent());
        assertEquals(PaymentPorts.CompletionResult.COMPLETED, attempts.complete(lease,
            new br.com.globo.checkout.worker.domain.PaymentResult(terminalCart,
                br.com.globo.checkout.worker.domain.PaymentResult.Outcome.APPROVED, java.time.Instant.now(), "Pix",
                null, null, BigDecimal.TEN, "BRL", List.of(), null)));
        assertEquals(1, cli.execute(new String[]{"authorize-processing", "--attempt-id", terminalAttempt}));
        assertEquals(1, cli.execute(new String[]{"authorize-publication", "--attempt-id", terminalAttempt}));
        assertEquals(1, cli.execute(new String[]{"authorize-processing", "--attempt-id", UUID.randomUUID().toString()}));
        assertEquals(1, cli.execute(new String[]{"mark-intervention", "--attempt-id", terminalAttempt,
            "--failure-partition", "0", "--failure-offset", Long.toString(dltOffset)}));
    }

    private void persistV1Cart(String cartId) throws Exception {
        var cart = new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("basic", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL")),
            List.of(new Models.PaymentMethod(PaymentCommand.PIX)));
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject(cart); }
        redisTemplate.opsForHash().put("checkout:cart:" + cartId, "payload", Base64.getEncoder().encodeToString(bytes.toByteArray()));
        redisTemplate.opsForHash().put("checkout:cart:" + cartId, "status", "PENDENTE");
    }
}
