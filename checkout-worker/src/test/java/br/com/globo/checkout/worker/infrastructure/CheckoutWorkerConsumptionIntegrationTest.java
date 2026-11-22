package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.worker.CheckoutWorkerApplication;
import br.com.globo.checkout.worker.application.PaymentCommandHandler;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PaymentProcessor;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

/** Exercises the independently configured worker context against real Kafka and Redis. */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(classes = CheckoutWorkerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {"spring.kafka.admin.auto-create=false", "checkout.kafka.lease-renewal-interval-ms=10000",
        "checkout.kafka.recovery-interval-ms=1000"})
class CheckoutWorkerConsumptionIntegrationTest {
    private static final String TOPIC = "checkout-v2-commands";
    private static final String FAILURE_TOPIC = "checkout-v2-failures";
    private static final String GROUP = "checkout-worker-integration";

    @Container static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"))
        .withEnv("KAFKA_OPTS", "-XX:-UseBASE64Intrinsics");
    @Container static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379);

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> GROUP);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.data.redis.timeout", () -> "500ms");
        registry.add("spring.data.redis.connect-timeout", () -> "500ms");
    }

    @BeforeAll static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1), new NewTopic(FAILURE_TOPIC, 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired StringRedisTemplate redisTemplate;
    @Autowired PaymentPorts.AttemptStore attempts;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired PaymentPorts.CommandPublisher commandPublisher;
    @MockitoSpyBean PaymentPorts.FailureQueuePublisher failureQueuePublisher;
    @MockitoSpyBean PaymentProcessor paymentProcessor;
    @Autowired KafkaListenerEndpointRegistry listeners;
    @MockitoSpyBean PaymentCommandHandler commandHandler;
    @Autowired ObjectMapper mapper;

    @Test void realWorkerStartsConsumesV1SnapshotAndCommitsDuplicateWithoutReexecution() throws Exception {
        assertFalse(listeners.getListenerContainers().isEmpty());
        assertTrue(listeners.getListenerContainers().stream().allMatch(container -> container.isRunning()));
        stopWorkerListener();
        resetSharedStateAndOffsets();

        String cartId = UUID.randomUUID().toString();
        String attemptId = UUID.randomUUID().toString();
        var cart = new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("basic", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL")),
            List.of(new Models.PaymentMethod(PaymentCommand.CREDIT), new Models.PaymentMethod(PaymentCommand.PIX)));
        redisTemplate.opsForHash().put("checkout:cart:" + cartId, "payload", serialize(cart));
        redisTemplate.opsForHash().put("checkout:cart:" + cartId, "status", "PENDENTE");

        PaymentCommand command = new PaymentCommand(1, attemptId, cartId, PaymentCommand.PIX, null);
        // No command record is sent directly. The recovery scheduler must discover the Redis-only reservation.
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(command));
        String wire = mapper.writeValueAsString(command.toWireFields());
        assertFalse(mapper.readTree(wire).has("lastDigits"), "Pix wire format must omit lastDigits");

        long firstCommandOffset;
        try (KafkaConsumer<String, String> uncommitted = consumer("uncommitted-" + attemptId)) {
        var commandPartition = new org.apache.kafka.common.TopicPartition(TOPIC, 0);
        uncommitted.assign(List.of(commandPartition));
        uncommitted.seekToEnd(List.of(commandPartition));
        Double recoveryDue = redisTemplate.opsForZSet().score("checkout:v2:publication-due", attemptId);
        assertNotNull(recoveryDue);
        long waitUntilBeforeDue = recoveryDue.longValue() - System.currentTimeMillis() - 1_500;
        if (waitUntilBeforeDue > 0) Thread.sleep(waitUntilBeforeDue);
        assertTrue(attempts.findPublicationDue(20).isEmpty(), "scheduler must not publish this crash recovery before 30 seconds");
        assertEquals(0, attempts.find(attemptId).orElseThrow().publicationCount());

        // Simulate a worker crash after durable result storage but before offset commit.
        var record = pollOne(uncommitted);
        assertEquals(cartId, record.key());
        firstCommandOffset = record.offset();
        assertEquals(PaymentCommandHandler.Outcome.ACKNOWLEDGE, commandHandler.handle(record.key(), record.value()));
        assertEquals(1, attempts.find(attemptId).orElseThrow().executionCount());
        // Closing without commit leaves this exact record for the production listener to redeliver.
        setGroupOffset(record.offset());
        }
        startWorkerListener();
        await(() -> committedOffset() >= firstCommandOffset + 1);
        await(() -> attempts.find(attemptId).map(s -> "APROVADO".equals(s.state()) && s.result() != null).orElse(false));
        assertEquals(1, attempts.find(attemptId).orElseThrow().executionCount());

        // A fresh Kafka delivery of the same command is acknowledged from the durable result.
        long duplicateOffset = kafkaTemplate.send(new ProducerRecord<>(TOPIC, cartId, wire)).get(10, TimeUnit.SECONDS).getRecordMetadata().offset();
        await(() -> committedOffset() >= duplicateOffset + 1);

        String cardCartId = UUID.randomUUID().toString();
        String cardAttemptId = UUID.randomUUID().toString();
        persistV1Cart(cardCartId);
        PaymentCommand card = new PaymentCommand(1, cardAttemptId, cardCartId, PaymentCommand.CREDIT, "4242");
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(card));
        String cardWire = mapper.writeValueAsString(card.toWireFields());
        assertEquals(5, mapper.readTree(cardWire).size());
        // A normal initial request uses the immediate publication path; recovery above had no Kafka send.
        kafkaTemplate.send(new ProducerRecord<>(TOPIC, cardCartId, cardWire)).get(10, TimeUnit.SECONDS);
        await(() -> attempts.find(cardAttemptId).map(s -> "APROVADO".equals(s.state()) && s.result() != null).orElse(false));
        long cardDuplicateOffset = kafkaTemplate.send(new ProducerRecord<>(TOPIC, cardCartId, cardWire)).get(10, TimeUnit.SECONDS).getRecordMetadata().offset();
        await(() -> committedOffset() >= cardDuplicateOffset + 1);

        var completed = attempts.find(attemptId).orElseThrow();
        var completedCard = attempts.find(cardAttemptId).orElseThrow();
        assertEquals("APROVADO", completed.state());
        assertNotNull(completed.result());
        assertEquals(1, completed.executionCount());
        assertEquals("APROVADO", completedCard.state());
        assertNotNull(completedCard.result());
        assertEquals(1, completedCard.executionCount());
        assertEquals(cardDuplicateOffset + 1, committedOffset());
    }

    @Test void invalidCommandIsForwardedToRealFailureTopicWithoutItsPayloadBeforeCommit() throws Exception {
        stopWorkerListener();
        long failureOffsetBefore;
        try (KafkaConsumer<String, String> failureReader = consumer("failure-reader-" + UUID.randomUUID())) {
            var partition = new org.apache.kafka.common.TopicPartition(FAILURE_TOPIC, 0);
            failureReader.assign(List.of(partition));
            failureReader.seekToEnd(List.of(partition));
            failureOffsetBefore = failureReader.position(partition);

            String secret = "{\"pan\":\"4111111111111111\",\"cvv\":\"123\"}";
            var sent = kafkaTemplate.send(new ProducerRecord<>(TOPIC, "untrusted-key", secret)).get(10, TimeUnit.SECONDS);
            setGroupOffset(sent.getRecordMetadata().offset());
            startWorkerListener();
            await(() -> committedOffset() >= sent.getRecordMetadata().offset() + 1);

            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> forwarded = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline && forwarded == null) {
                var records = failureReader.poll(Duration.ofMillis(200));
                for (var record : records) if (record.offset() >= failureOffsetBefore) { forwarded = record; break; }
            }
            assertNotNull(forwarded, "DLT send must be confirmed before source offset commit");
            var payload = mapper.readTree(forwarded.value());
            assertEquals(1, payload.get("schemaVersion").asInt());
            assertEquals(TOPIC, payload.get("sourceTopic").asText());
            assertEquals(0, payload.get("sourcePartition").asInt());
            assertEquals(sent.getRecordMetadata().offset(), payload.get("sourceOffset").asLong());
            assertEquals("INVALID_COMMAND", payload.get("reasonCode").asText());
            assertFalse(payload.has("attemptId"));
            assertFalse(payload.has("cartId"));
            assertFalse(forwarded.value().contains("4111111111111111"));
            assertFalse(forwarded.value().contains("123"));
        }
    }

    @Test void redisOutageWaitsToPersistInterventionBeforeForwarding() throws Exception {
        stopWorkerListener();
        resetSharedStateAndOffsets();
        String cartId = UUID.randomUUID().toString();
        String attemptId = UUID.randomUUID().toString();
        persistV1Cart(cartId);
        PaymentCommand command = new PaymentCommand(1, attemptId, cartId, PaymentCommand.CREDIT, "4242");
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, attempts.reservePublication(attemptId));
        assertTrue(attempts.confirmPublication(attemptId));

        long sourceOffset = kafkaTemplate.send(TOPIC, cartId, mapper.writeValueAsString(command))
            .get(10, TimeUnit.SECONDS).getRecordMetadata().offset();
        setGroupOffset(sourceOffset);
        var failurePartition = new org.apache.kafka.common.TopicPartition(FAILURE_TOPIC, 0);
        try (KafkaConsumer<String, String> failureReader = consumer("redis-outage-dlt-reader-" + UUID.randomUUID())) {
        failureReader.assign(List.of(failurePartition));
        failureReader.seekToEnd(List.of(failurePartition));
        long dltStart = failureReader.position(failurePartition);
        DockerClientFactory.instance().client().pauseContainerCmd(redis.getContainerId()).exec();
        boolean paused = true;
        AtomicInteger redisUnavailableHandlingCount = new AtomicInteger();
        CountDownLatch outageResolutionFailed = new CountDownLatch(1);
        reset(commandHandler);
        doAnswer(invocation -> {
            var result = (PaymentCommandHandler.HandlingResult) invocation.callRealMethod();
            if (attemptId.equals(result.attemptId())
                && result.outcome() == PaymentCommandHandler.Outcome.REDIS_UNAVAILABLE) {
                redisUnavailableHandlingCount.incrementAndGet();
            }
            return result;
        }).when(commandHandler).handleDetailed(eq(cartId), anyString());
        doAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (RuntimeException redisStillUnavailable) {
                outageResolutionFailed.countDown();
                throw redisStillUnavailable;
            }
        }).when(commandHandler).resolveRedisFailure(eq(cartId), anyString());
        try {
            startWorkerListener();
            long checksDeadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < checksDeadline
                && (redisUnavailableHandlingCount.get() < 3 || outageResolutionFailed.getCount() != 0)) {
                Thread.sleep(25);
            }
            assertEquals(3, redisUnavailableHandlingCount.get(),
                "the specific source record must complete three Redis-unavailable checks before Redis is restored");
            assertEquals(0, outageResolutionFailed.getCount(),
                "the worker must attempt and fail Redis state resolution while Redis is still paused");
            assertTrue(committedOffset() <= sourceOffset, "source offset remains uncommitted while Redis is unavailable");
            assertEquals(dltStart, endOffset(FAILURE_TOPIC), "no DLT is sent before the intervention can be persisted");

            DockerClientFactory.instance().client().unpauseContainerCmd(redis.getContainerId()).exec();
            paused = false;
            await(() -> "INTERVENCAO".equals(attempts.find(attemptId).orElseThrow().state()));
            assertEquals("REDIS_UNAVAILABLE", attempts.find(attemptId).orElseThrow().safeReason());
            await(() -> committedOffset() > sourceOffset);
            assertEquals(0, attempts.find(attemptId).orElseThrow().executionCount(), "outage handling never executes payment");
            var records = failureReader.poll(Duration.ofSeconds(2));
            var dlt = records.records(new org.apache.kafka.common.TopicPartition(FAILURE_TOPIC, 0)).stream()
                .filter(record -> record.offset() >= dltStart).findFirst().orElse(null);
            assertNotNull(dlt, "persisted intervention is forwarded after Redis recovery");
            var failure = mapper.readTree(dlt.value());
            assertEquals("REDIS_UNAVAILABLE", failure.get("reasonCode").asText());
            assertEquals(sourceOffset, failure.get("sourceOffset").asLong());
        } finally {
            if (paused) DockerClientFactory.instance().client().unpauseContainerCmd(redis.getContainerId()).exec();
            if (listeners.getListenerContainers().stream().noneMatch(container -> container.isRunning())) startWorkerListener();
        }
        }
    }
    @Test void lostDltConfirmationKeepsSourceOffsetPendingAndRedeliveryDoesNotExecuteFourthTime() throws Exception {
        stopWorkerListener();
        resetSharedStateAndOffsets();
        String cartId = UUID.randomUUID().toString();
        String attemptId = UUID.randomUUID().toString();
        persistV1Cart(cartId);
        PaymentCommand command = new PaymentCommand(1, attemptId, cartId, PaymentCommand.CREDIT, "4242");
        assertEquals(PaymentPorts.ReservationResult.CREATED, attempts.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, attempts.reservePublication(attemptId));
        assertTrue(attempts.confirmPublication(attemptId));

        AtomicBoolean confirmDlt = new AtomicBoolean(false);
        doAnswer(invocation -> {
            var lease = invocation.getArgument(1, PaymentPorts.ProcessingLease.class);
            assertTrue(attempts.reserveExecution(lease).isPresent());
            throw new IllegalStateException("controlled transient processing error");
        }).when(paymentProcessor).process(any(), any());
        doAnswer(invocation -> {
            boolean brokerConfirmed = (boolean) invocation.callRealMethod();
            return brokerConfirmed && confirmDlt.get();
        }).when(failureQueuePublisher).publish(any(), any());

        long failureOffsetStart;
        try (KafkaConsumer<String, String> failureReader = consumer("failure-ack-reader-" + UUID.randomUUID())) {
            var failurePartition = new org.apache.kafka.common.TopicPartition(FAILURE_TOPIC, 0);
            failureReader.assign(List.of(failurePartition));
            failureReader.seekToEnd(List.of(failurePartition));
            failureOffsetStart = failureReader.position(failurePartition);

            long sourceOffset = kafkaTemplate.send(new ProducerRecord<>(TOPIC, cartId,
                mapper.writeValueAsString(command.toWireFields()))).get(10, TimeUnit.SECONDS).getRecordMetadata().offset();
            setGroupOffset(sourceOffset);
            startWorkerListener();
            java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> forwarded = new java.util.ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
            while (System.nanoTime() < deadline && forwarded.size() < 2) {
                for (var record : failureReader.poll(Duration.ofMillis(200)))
                    if (record.offset() >= failureOffsetStart) forwarded.add(record);
            }
            assertTrue(forwarded.size() >= 2, "unconfirmed forwarding is retried and may duplicate in the DLT");
            assertEquals(sourceOffset, committedOffset(), "source offset must remain uncommitted until DLT acknowledgment is observed");
            var state = attempts.find(attemptId).orElseThrow();
            assertEquals("INTERVENCAO", state.state());
            assertEquals(3, state.executionCount());
            verify(paymentProcessor, times(3)).process(any(), any());
            for (var failureRecord : forwarded) {
                var notice = mapper.readTree(failureRecord.value());
                assertEquals(sourceOffset, notice.get("sourceOffset").asLong());
                assertEquals("EXECUTION_EXHAUSTED", notice.get("reasonCode").asText());
                assertFalse(failureRecord.value().contains("4242"));
            }

            confirmDlt.set(true);
            await(() -> committedOffset() >= sourceOffset + 1);
            assertEquals(3, attempts.find(attemptId).orElseThrow().executionCount());
            verify(paymentProcessor, times(3)).process(any(), any());
        } finally {
            confirmDlt.set(true);
            reset(paymentProcessor, failureQueuePublisher);
            if (listeners.getListenerContainers().stream().noneMatch(container -> container.isRunning())) startWorkerListener();
        }
    }

    private long committedOffset() {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            return offsets.values().stream().mapToLong(value -> value.offset()).max().orElse(0L);
        } catch (Exception unavailableUntilGroupIsAssigned) {
            return 0;
        }
    }

    private void setGroupOffset(long offset) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var partition = new org.apache.kafka.common.TopicPartition(TOPIC, 0);
            admin.alterConsumerGroupOffsets(GROUP, Map.of(partition,
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset))).all().get(10, TimeUnit.SECONDS);
        }
    }

    private long endOffset(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var partition = new org.apache.kafka.common.TopicPartition(topic, 0);
            return admin.listOffsets(Map.of(partition, org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get(5, TimeUnit.SECONDS).get(partition).offset();
        }
    }

    private void resetSharedStateAndOffsets() throws Exception {
        try (var connection = redisTemplate.getConnectionFactory().getConnection()) { connection.serverCommands().flushDb(); }
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var commandPartition = new org.apache.kafka.common.TopicPartition(TOPIC, 0);
            var failurePartition = new org.apache.kafka.common.TopicPartition(FAILURE_TOPIC, 0);
            admin.alterConsumerGroupOffsets(GROUP, Map.of(commandPartition,
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(0))).all().get(10, TimeUnit.SECONDS);
        }
    }

    private KafkaConsumer<String, String> consumer(String groupId) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }

    private org.apache.kafka.clients.consumer.ConsumerRecord<String, String> pollOne(KafkaConsumer<String, String> consumer)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(200));
            if (!records.isEmpty()) return records.iterator().next();
        }
        fail("Timed out waiting for uncommitted Kafka record");
        throw new AssertionError("unreachable");
    }

    private void stopWorkerListener() throws InterruptedException {
        listeners.getListenerContainers().forEach(container -> container.stop());
        await(() -> listeners.getListenerContainers().stream().noneMatch(container -> container.isRunning()));
    }

    private void startWorkerListener() throws InterruptedException {
        listeners.getListenerContainers().forEach(container -> container.start());
        await(() -> listeners.getListenerContainers().stream().allMatch(container -> container.isRunning()));
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "Timed out waiting for worker state/offset");
    }

    private static String serialize(Models.Cart cart) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject(cart); }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private void persistV1Cart(String cartId) throws Exception {
        var cart = new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("basic", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL")),
            List.of(new Models.PaymentMethod(PaymentCommand.CREDIT), new Models.PaymentMethod(PaymentCommand.PIX)));
        redisTemplate.opsForHash().put("checkout:cart:" + cartId, "payload", serialize(cart));
        redisTemplate.opsForHash().put("checkout:cart:" + cartId, "status", "PENDENTE");
    }
}
