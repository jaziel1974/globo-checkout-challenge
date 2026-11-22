package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.worker.application.PaymentCommandHandler;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PaymentProcessor;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
import br.com.globo.checkout.worker.application.ScheduledLeaseRenewer;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import br.com.globo.checkout.worker.domain.PaymentSimulator;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

@Testcontainers(disabledWithoutDocker = true)
class RedisAttemptStoreIntegrationTest {
    @Container static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    static LettuceConnectionFactory connectionFactory;
    static StringRedisTemplate template;
    RedisAttemptStore first;
    RedisAttemptStore second;

    @BeforeAll static void startRedis() {
        connectionFactory = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        template = new StringRedisTemplate(connectionFactory);
        template.afterPropertiesSet();
    }
    @AfterAll static void stopRedis() { if (connectionFactory != null) connectionFactory.destroy(); redis.stop(); }
    @BeforeEach void stores() {
        try (var connection = template.getConnectionFactory().getConnection()) { connection.serverCommands().flushDb(); }
        first = new RedisAttemptStore(template, new ObjectMapper()); second = new RedisAttemptStore(template, new ObjectMapper());
    }

    @Test void createsOneActiveAttemptAtomicallyAndKeepsItsFieldsPrivateToV2() throws Exception {
        String cartId = createCart();
        var pool = Executors.newFixedThreadPool(16);
        try {
            List<java.util.concurrent.Future<PaymentPorts.ReservationResult>> results = java.util.stream.IntStream.range(0, 32)
                .mapToObj(i -> pool.submit(() -> (i % 2 == 0 ? first : second).create(command(cartId))))
                .toList();
            long created = results.stream().filter(f -> { try { return f.get(10, TimeUnit.SECONDS) == PaymentPorts.ReservationResult.CREATED; } catch (Exception e) { throw new RuntimeException(e); } }).count();
            assertEquals(1, created);
        } finally { pool.shutdownNow(); }

        var snapshot = first.findActiveByCart(cartId).orElseThrow();
        String attemptKey = "checkout:v2:attempt:" + snapshot.attemptId();
        assertEquals("AGUARDANDO_PUBLICACAO", snapshot.state());
        assertNotNull(template.opsForZSet().score("checkout:v2:publication-due", snapshot.attemptId()));
        assertEquals(-1, template.getExpire(attemptKey));
        MapAssertions.assertNoSensitiveFields(template.opsForHash().entries(attemptKey).toString());
        assertFalse(template.hasKey("checkout:cart:" + cartId + ":approved"));
        assertEquals("PENDENTE", template.opsForHash().get("checkout:cart:" + cartId, "status"));
    }

    @Test void recoveryIndexWaitsThirtySecondsWhileInitialPublicationRemainsImmediatelyEligible() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));

        long now = System.currentTimeMillis();
        Double dueAt = template.opsForZSet().score("checkout:v2:publication-due", command.attemptId());
        assertNotNull(dueAt);
        assertTrue(dueAt >= now + 29_000 && dueAt <= now + 31_000, "recovery index must honor the 30-second delay");
        assertTrue(first.findPublicationDue(20).isEmpty(), "new reservation must not be recovered early");

        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()),
            "the immediate BFF publication may reserve independently of the delayed recovery index");
        assertEquals(1, first.find(command.attemptId()).orElseThrow().publicationCount());
    }

    @Test void interventionFencesAcquisitionAndCannotBeOverwrittenByStaleCompletion() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertTrue(first.confirmPublication(command.attemptId()));
        var staleLease = first.acquire(command.attemptId()).orElseThrow();

        assertTrue(second.markIntervention(command.attemptId(), "REDIS_UNAVAILABLE"));
        assertTrue(second.acquire(command.attemptId()).isEmpty(), "intervention must fence a new lease");
        assertEquals(PaymentPorts.CompletionResult.LOST_OWNERSHIP,
            first.complete(staleLease, approvedResult(cartId)),
            "a stale payment result cannot replace an intervention persisted before DLT forwarding");
        assertEquals("INTERVENCAO", second.find(command.attemptId()).orElseThrow().state());
        assertEquals("REDIS_UNAVAILABLE", second.find(command.attemptId()).orElseThrow().safeReason());
    }
    @Test void manualProcessingAuthorizationDoesNotAdvancePublicationBackoff() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertTrue(first.confirmPublication(command.attemptId()));
        long originalNextPublication = first.find(command.attemptId()).orElseThrow().nextPublicationAtMillis();
        assertTrue(originalNextPublication > System.currentTimeMillis());
        assertTrue(first.markIntervention(command.attemptId(), "EXECUTION_EXHAUSTED"));

        assertTrue(first.authorizeProcessingCycle(command.attemptId()));
        assertEquals(originalNextPublication, first.find(command.attemptId()).orElseThrow().nextPublicationAtMillis());
        assertTrue(first.findPublicationDue(20).isEmpty(), "operator authorization must not bypass the remaining 30-second publication interval");
        assertEquals(originalNextPublication, template.opsForZSet().score("checkout:v2:publication-due", command.attemptId()).longValue());
    }

    @Test void doesNotReplaceAnActiveLinkWhoseAttemptRecordIsMissing() {
        String cartId = createCart();
        String missingAttempt = UUID.randomUUID().toString();
        template.opsForHash().put("checkout:v2:cart:" + cartId, "activeAttemptId", missingAttempt);
        PaymentCommand candidate = command(cartId);
        var result = first.create(candidate);
        assertEquals(PaymentPorts.ReservationResult.ACTIVE_ATTEMPT, result);
        assertEquals(missingAttempt, template.opsForHash().get("checkout:v2:cart:" + cartId, "activeAttemptId"));
        assertFalse(Boolean.TRUE.equals(template.hasKey("checkout:v2:attempt:" + candidate.attemptId())));
    }

    @Test void publicationReservationAndLateConfirmationDoNotRegressProcessingOrTerminalState() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertNotNull(template.opsForZSet().score("checkout:v2:publication-due", command.attemptId()));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertEquals(PaymentPorts.PublishReservation.NOT_DUE, second.reservePublication(command.attemptId()));
        assertEquals(1L, template.opsForZSet().zCard("checkout:v2:publication-due"));
        assertEquals("1", template.opsForHash().get("checkout:v2:attempt:" + command.attemptId(), "publicationCount"));

        var lease = second.acquire(command.attemptId()).orElseThrow();
        assertEquals("PROCESSANDO", first.find(command.attemptId()).orElseThrow().state());
        assertEquals(0L, template.opsForZSet().zCard("checkout:v2:publication-due"));
        assertTrue(first.confirmPublication(command.attemptId()));
        assertEquals("PROCESSANDO", second.find(command.attemptId()).orElseThrow().state());
        var result = new PaymentResult(cartId, PaymentResult.Outcome.APPROVED, java.time.Instant.now(),
            PaymentCommand.CREDIT, "4242", null, new BigDecimal("19.90"), "BRL",
            List.of(new PaymentResult.ProductResult("basico", "Plano Básico", new BigDecimal("19.90"))), null);
        assertEquals(PaymentPorts.CompletionResult.COMPLETED, first.complete(lease, result));
        assertTrue(second.confirmPublication(command.attemptId()));
        assertEquals("APROVADO", second.find(command.attemptId()).orElseThrow().state());
        assertEquals(command.attemptId(), template.opsForHash().get("checkout:v2:cart:" + cartId, "approvedAttemptId"));
        assertTrue(first.isApprovedByCart(cartId));
        assertEquals("PENDENTE", template.opsForHash().get("checkout:cart:" + cartId, "status"));
        assertEquals(0L, template.opsForZSet().zCard("checkout:v2:publication-due"));
    }

    @Test void possessionRenewalReplacementAndThreeExecutionLimitAreAtomic() throws Exception {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        var oldLease = first.acquire(command.attemptId()).orElseThrow();
        assertFalse(second.acquire(command.attemptId()).isPresent());
        assertTrue(first.renew(oldLease));
        String attemptKey = "checkout:v2:attempt:" + command.attemptId();
        template.opsForHash().put(attemptKey, "leaseUntil", "0");
        var newLease = second.acquire(command.attemptId()).orElseThrow();
        assertNotEquals(oldLease.ownerToken(), newLease.ownerToken());
        assertFalse(first.renew(oldLease));

        assertEquals(1, second.reserveExecution(newLease).orElseThrow());
        Thread.sleep(1_010);
        assertEquals(2, second.reserveExecution(newLease).orElseThrow());
        Thread.sleep(1_010);
        assertEquals(3, second.reserveExecution(newLease).orElseThrow());
        assertTrue(second.reserveExecution(newLease).isEmpty());
        assertEquals("3", template.opsForHash().get(attemptKey, "executionCount"));

        var staleResult = new PaymentResult(cartId, PaymentResult.Outcome.APPROVED, java.time.Instant.now(),
            PaymentCommand.CREDIT, "4242", null, new BigDecimal("19.90"), "BRL", List.of(), null);
        assertEquals(PaymentPorts.CompletionResult.LOST_OWNERSHIP, first.complete(oldLease, staleResult));
        assertEquals("PROCESSANDO", second.find(command.attemptId()).orElseThrow().state());
    }

    @Test void workerHandlerTakeoverPreventsExpiredOwnerFromCompleting() throws Exception {
        String cartId = createValidCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        String wire = new ObjectMapper().writeValueAsString(command.toWireFields());

        PaymentProcessor actualProcessor = new PaymentProcessor(second, new RedisCartSnapshotRepository(template), new PaymentSimulator());
        PaymentProcessor delayedOldProcessor = mock(PaymentProcessor.class);
        CountDownLatch oldOwnerEntered = new CountDownLatch(1);
        CountDownLatch allowOldOwnerToContinue = new CountDownLatch(1);
        AtomicReference<PaymentPorts.ProcessingLease> oldLease = new AtomicReference<>();
        when(delayedOldProcessor.process(any(PaymentCommand.class), any(PaymentPorts.ProcessingLease.class)))
            .thenAnswer(invocation -> {
                PaymentPorts.ProcessingLease lease = invocation.getArgument(1);
                oldLease.set(lease);
                oldOwnerEntered.countDown();
                if (!allowOldOwnerToContinue.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("controlled wait expired");
                return actualProcessor.process(invocation.getArgument(0), lease);
            });

        var oldRenewal = Executors.newSingleThreadScheduledExecutor();
        var newRenewal = Executors.newSingleThreadScheduledExecutor();
        var handlers = Executors.newSingleThreadExecutor();
        try {
            var oldHandler = new PaymentCommandHandler(new ObjectMapper(), first, delayedOldProcessor,
                new ScheduledLeaseRenewer(first, oldRenewal, 60_000));
            var newHandler = new PaymentCommandHandler(new ObjectMapper(), second, actualProcessor,
                new ScheduledLeaseRenewer(second, newRenewal, 60_000));
            var oldDelivery = handlers.submit(() -> oldHandler.handle(cartId, wire));
            assertTrue(oldOwnerEntered.await(5, TimeUnit.SECONDS));

            template.opsForHash().put("checkout:v2:attempt:" + command.attemptId(), "leaseUntil", "0");
            assertEquals(PaymentCommandHandler.Outcome.ACKNOWLEDGE, newHandler.handle(cartId, wire));
            allowOldOwnerToContinue.countDown();

            // Result is already durable under the new owner's lease, so the stale duplicate is safe to ACK.
            assertEquals(PaymentCommandHandler.Outcome.ACKNOWLEDGE, oldDelivery.get(5, TimeUnit.SECONDS));
            assertFalse(first.renew(oldLease.get()));
            var completed = first.find(command.attemptId()).orElseThrow();
            assertEquals("APROVADO", completed.state());
            assertEquals(1, completed.executionCount());
            assertNotNull(completed.result());
        } finally {
            allowOldOwnerToContinue.countDown();
            oldRenewal.shutdownNow(); newRenewal.shutdownNow(); handlers.shutdownNow();
        }
    }

    @Test void redeliveryAfterInterventionNeverInvokesPaymentProcessor() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        String attemptKey = "checkout:v2:attempt:" + command.attemptId();
        for (int i = 0; i < 3; i++) {
            template.opsForHash().put(attemptKey, "nextPublicationAt", "0");
            assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        }
        assertTrue(first.markPublicationExhausted(command.attemptId()));

        PaymentProcessor processor = mock(PaymentProcessor.class);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), first, processor,
                new ScheduledLeaseRenewer(first, scheduler, 60_000));
            String wire = new ObjectMapper().writeValueAsString(command.toWireFields());
            assertEquals(PaymentCommandHandler.Outcome.INTERVENTION, handler.handle(cartId, wire));
            verifyNoInteractions(processor);
            assertEquals("INTERVENCAO", first.find(command.attemptId()).orElseThrow().state());
            assertEquals(0, first.find(command.attemptId()).orElseThrow().executionCount());
        } finally { scheduler.shutdownNow(); }
    }

    @Test void publicationRetriesAreCountedBeforeSendingAndEndInIntervention() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        String attemptKey = "checkout:v2:attempt:" + command.attemptId();
        for (int expectedCount = 1; expectedCount <= 3; expectedCount++) {
            template.opsForHash().put(attemptKey, "nextPublicationAt", "0");
            assertEquals(PaymentPorts.PublishReservation.RESERVED, (expectedCount == 1 ? first : second).reservePublication(command.attemptId()));
            assertEquals(Integer.toString(expectedCount), template.opsForHash().get(attemptKey, "publicationCount"));
        }
        template.opsForHash().put(attemptKey, "nextPublicationAt", "0");
        assertEquals(PaymentPorts.PublishReservation.EXHAUSTED, first.reservePublication(command.attemptId()));
        assertEquals("INTERVENCAO", first.find(command.attemptId()).orElseThrow().state());
        assertEquals(0L, template.opsForZSet().zCard("checkout:v2:publication-due"));
        assertTrue(first.markIntervention(command.attemptId(), "OPERATOR_REVIEW"));
    }

    @Test void concurrentPublicationCoordinatorsAuthorizeOnlyOneRealKafkaCall() throws Exception {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        java.util.concurrent.atomic.AtomicInteger sends = new java.util.concurrent.atomic.AtomicInteger();
        PaymentPorts.CommandPublisher publisher = (reservedCommand, timeout) -> {
            sends.incrementAndGet();
            return PaymentPorts.PublishResult.CONFIRMED;
        };
        var one = new PublicationCoordinator(first, publisher);
        var two = new PublicationCoordinator(second, publisher);
        var gate = new java.util.concurrent.CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var a = pool.submit(() -> { gate.await(); return one.publish(command.attemptId(), java.time.Duration.ofSeconds(2)); });
            var b = pool.submit(() -> { gate.await(); return two.publish(command.attemptId(), java.time.Duration.ofSeconds(2)); });
            gate.countDown();
            var results = List.of(a.get(5, TimeUnit.SECONDS), b.get(5, TimeUnit.SECONDS));
            assertEquals(1, sends.get());
            assertEquals(1, results.stream().filter(r -> r == PublicationCoordinator.Result.CONFIRMED).count());
            assertTrue(results.stream().anyMatch(r -> r == PublicationCoordinator.Result.NOT_DUE));
        } finally { pool.shutdownNow(); }
        var persisted = first.find(command.attemptId()).orElseThrow();
        assertEquals(1, persisted.publicationCount());
        assertEquals("PUBLICADO", persisted.state());
    }

    @Test void applicationCrashAfterReservationConsumesCountBeforeAnySend() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        var persisted = first.find(command.attemptId()).orElseThrow();
        assertEquals(1, persisted.publicationCount());
        assertEquals("AGUARDANDO_PUBLICACAO", persisted.state());
        assertTrue(persisted.nextPublicationAtMillis() >= System.currentTimeMillis());
        assertTrue(persisted.nextPublicationAtMillis() <= System.currentTimeMillis() + 35_000L);
        assertEquals(0L, persisted.nextExecutionAtMillis());
    }

    @Test void publicationExhaustionAndCompletionRacePreserveOnlyTheAtomicWinner() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        String attemptKey = "checkout:v2:attempt:" + command.attemptId();
        for (int i = 0; i < 3; i++) {
            template.opsForHash().put(attemptKey, "nextPublicationAt", "0");
            assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        }
        var lease = second.acquire(command.attemptId()).orElseThrow();
        assertTrue(first.markPublicationExhausted(command.attemptId()));
        assertEquals("INTERVENCAO", first.find(command.attemptId()).orElseThrow().state());
        var result = approvedResult(cartId);
        assertEquals(PaymentPorts.CompletionResult.LOST_OWNERSHIP, second.complete(lease, result));
        assertNull(template.opsForHash().get(attemptKey, "result"));

        String completedCart = createCart();
        PaymentCommand completedCommand = command(completedCart);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(completedCommand));
        String completedAttemptKey = "checkout:v2:attempt:" + completedCommand.attemptId();
        for (int i = 0; i < 3; i++) {
            template.opsForHash().put(completedAttemptKey, "nextPublicationAt", "0");
            assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(completedCommand.attemptId()));
        }
        var completedLease = second.acquire(completedCommand.attemptId()).orElseThrow();
        assertEquals(PaymentPorts.CompletionResult.COMPLETED, second.complete(completedLease, approvedResult(completedCart)));
        assertFalse(first.markPublicationExhausted(completedCommand.attemptId()));
        assertEquals("APROVADO", first.find(completedCommand.attemptId()).orElseThrow().state());
        assertEquals(completedCommand.attemptId(), template.opsForHash().get("checkout:v2:cart:" + completedCart, "approvedAttemptId"));
    }

    @Test void completionRejectsAnAttemptThatNoLongerOwnsTheCartLink() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        var lease = first.acquire(command.attemptId()).orElseThrow();
        template.opsForHash().put("checkout:v2:cart:" + cartId, "activeAttemptId", UUID.randomUUID().toString());
        assertEquals(PaymentPorts.CompletionResult.LOST_OWNERSHIP, first.complete(lease, approvedResult(cartId)));
        assertNull(template.opsForHash().get("checkout:v2:attempt:" + command.attemptId(), "result"));
        assertNull(template.opsForHash().get("checkout:v2:cart:" + cartId, "approvedAttemptId"));
    }

    @Test void lossOfRedisStateDoesNotRecreateAnOldAttemptOrCart() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        try (var connection = template.getConnectionFactory().getConnection()) { connection.serverCommands().flushDb(); }
        assertTrue(first.find(command.attemptId()).isEmpty());
        assertTrue(first.acquire(command.attemptId()).isEmpty());
        assertEquals(PaymentPorts.ReservationResult.CART_NOT_FOUND, first.create(command));
        assertFalse(Boolean.TRUE.equals(template.hasKey("checkout:v2:attempt:" + command.attemptId())));
    }

    @Test void missingApprovedAttemptRecordStillFailsClosedThroughDurableCartMarker() {
        String cartId = createCart();
        template.opsForHash().put("checkout:v2:cart:" + cartId, "approvedAttemptId", UUID.randomUUID().toString());
        assertTrue(first.isApprovedByCart(cartId));
        assertTrue(first.findActiveByCart(cartId).isEmpty());
    }

    @Test void recusalReleasesV2CartForANewAttemptWithoutChangingV1Cart() {
        String cartId = createCart();
        PaymentCommand firstCommand = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(firstCommand));
        var lease = first.acquire(firstCommand.attemptId()).orElseThrow();
        PaymentResult declined = new PaymentResult(cartId, PaymentResult.Outcome.DECLINED, null,
            PaymentCommand.CREDIT, "0002", null, new BigDecimal("19.90"), "BRL",
            List.of(new PaymentResult.ProductResult("basico", "Plano Básico", new BigDecimal("19.90"))), "PAYMENT_DECLINED");
        assertEquals(PaymentPorts.CompletionResult.COMPLETED, first.complete(lease, declined));
        assertEquals("RECUSADO", first.find(firstCommand.attemptId()).orElseThrow().state());

        PaymentCommand retry = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, second.create(retry));
        assertEquals(retry.attemptId(), second.findActiveByCart(cartId).orElseThrow().attemptId());
        assertEquals("PENDENTE", template.opsForHash().get("checkout:cart:" + cartId, "status"));
        assertNull(template.opsForHash().get("checkout:v2:cart:" + cartId, "approvedAttemptId"));
    }

    @Test void retryReleasePreservesOneSecondExecutionDueAndDoesNotWaitForLeaseExpiry() throws Exception {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertTrue(first.confirmPublication(command.attemptId()));
        var lease = first.acquire(command.attemptId()).orElseThrow();
        assertEquals(1, first.reserveExecution(lease).orElseThrow());
        long due = first.find(command.attemptId()).orElseThrow().nextExecutionAtMillis();
        assertTrue(first.releaseForRetry(lease));
        assertTrue(first.acquire(command.attemptId()).isEmpty(), "retry must honor the persisted one-second due time");
        long wait = due - System.currentTimeMillis() + 50;
        if (wait > 0) Thread.sleep(wait);
        assertTrue(first.acquire(command.attemptId()).isPresent(), "released token must allow prompt reacquisition after due time");
    }

    @Test void staleOwnerCannotExhaustInterventionAfterAnotherOwnerTakesTheThirdExecution() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertTrue(first.confirmPublication(command.attemptId()));
        var stale = first.acquire(command.attemptId()).orElseThrow();
        template.opsForHash().put("checkout:v2:attempt:" + command.attemptId(), "leaseUntil", "0");
        var current = second.acquire(command.attemptId()).orElseThrow();
        template.opsForHash().put("checkout:v2:attempt:" + command.attemptId(), "executionCount", "3");
        assertFalse(first.markExecutionExhausted(stale));
        assertEquals("PROCESSANDO", second.find(command.attemptId()).orElseThrow().state());
        assertTrue(second.markExecutionExhausted(current));
        assertEquals("INTERVENCAO", first.find(command.attemptId()).orElseThrow().state());
    }

    @Test void authorizationSeparatesPublicationBudgetFromProcessingCycle() {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        String key = "checkout:v2:attempt:" + command.attemptId();
        template.opsForHash().put(key, "publicationCount", "3");
        assertTrue(first.markIntervention(command.attemptId(), "EXECUTION_EXHAUSTED"));
        assertFalse(first.authorizeProcessingCycle(command.attemptId()), "processing auth cannot silently reset exhausted publication budget");
        assertTrue(first.authorizePublicationCycle(command.attemptId()));
        assertEquals("INTERVENCAO", first.find(command.attemptId()).orElseThrow().state());
        assertTrue(first.authorizeProcessingCycle(command.attemptId()));
        var next = first.find(command.attemptId()).orElseThrow();
        assertEquals("AGUARDANDO_PUBLICACAO", next.state());
        assertEquals(0, next.publicationCount());
        assertEquals(0, next.executionCount());
        assertTrue(first.reservePublication(command.attemptId()) == PaymentPorts.PublishReservation.RESERVED);
    }

    @Test void threeExecutionRetriesAreCountedAndSeparatedByAboutOneSecond() throws Exception {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertTrue(first.confirmPublication(command.attemptId()));
        var processor = mock(PaymentProcessor.class);
        doAnswer(invocation -> {
            var lease = invocation.getArgument(1, PaymentPorts.ProcessingLease.class);
            assertTrue(first.reserveExecution(lease).isPresent());
            throw new IllegalStateException("simulated transient processing failure");
        }).when(processor).process(any(), any());
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), first, processor,
                new ScheduledLeaseRenewer(first, scheduler, 10_000));
            String wire = new ObjectMapper().writeValueAsString(command.toWireFields());
            long[] start = new long[3];
            for (int i = 0; i < 3; i++) {
                var outcome = handler.handleDetailed(cartId, wire);
                if (i < 2) assertEquals(PaymentCommandHandler.Outcome.RETRY, outcome.outcome());
                else assertEquals(PaymentCommandHandler.Outcome.INTERVENTION, outcome.outcome());
                var snapshot = first.find(command.attemptId()).orElseThrow();
                assertEquals(i + 1, snapshot.executionCount());
                start[i] = snapshot.nextExecutionAtMillis() - 1_000;
                if (i < 2) {
                    long wait = snapshot.nextExecutionAtMillis() - System.currentTimeMillis() + 30;
                    if (wait > 0) Thread.sleep(wait);
                }
            }
            assertTrue(start[1] - start[0] >= 900, "second execution must be delayed approximately one second");
            assertTrue(start[2] - start[1] >= 900, "third execution must be delayed approximately one second");
            assertEquals("INTERVENCAO", first.find(command.attemptId()).orElseThrow().state());
            assertEquals("EXECUTION_EXHAUSTED", first.find(command.attemptId()).orElseThrow().safeReason());
            assertEquals(3, mockingDetails(processor).getInvocations().size());
        } finally { scheduler.shutdownNow(); }
    }

    @Test void missingV1CartSnapshotIsQueuedForInterventionWithoutRecreatingIt() throws Exception {
        String cartId = createCart();
        PaymentCommand command = command(cartId);
        assertEquals(PaymentPorts.ReservationResult.CREATED, first.create(command));
        assertEquals(PaymentPorts.PublishReservation.RESERVED, first.reservePublication(command.attemptId()));
        assertTrue(first.confirmPublication(command.attemptId()));
        template.opsForHash().delete("checkout:cart:" + cartId, "payload");
        var processor = new PaymentProcessor(first, id -> java.util.Optional.empty(), new PaymentSimulator());
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), first, processor,
                new ScheduledLeaseRenewer(first, scheduler, 10_000));
            String wire = new ObjectMapper().writeValueAsString(command.toWireFields());
            var result = handler.handleDetailed(cartId, wire);
            assertEquals(PaymentCommandHandler.Outcome.INTERVENTION, result.outcome());
            assertEquals("CART_SNAPSHOT_MISSING", result.reasonCode());
            var state = first.find(command.attemptId()).orElseThrow();
            assertEquals("INTERVENCAO", state.state());
            assertEquals(1, state.executionCount());
            assertFalse(template.opsForHash().hasKey("checkout:cart:" + cartId, "payload"));
        } finally { scheduler.shutdownNow(); }
    }

    private String createCart() {
        String cartId = UUID.randomUUID().toString();
        template.opsForHash().putAll("checkout:cart:" + cartId, java.util.Map.of("payload", "v1-snapshot", "status", "PENDENTE"));
        return cartId;
    }
    private String createValidCart() throws Exception {
        String cartId = UUID.randomUUID().toString();
        var cart = new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("basic", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL")),
            List.of(new Models.PaymentMethod(PaymentCommand.CREDIT), new Models.PaymentMethod(PaymentCommand.PIX)));
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject(cart); }
        template.opsForHash().putAll("checkout:cart:" + cartId, java.util.Map.of(
            "payload", java.util.Base64.getEncoder().encodeToString(bytes.toByteArray()), "status", "PENDENTE"));
        return cartId;
    }
    private static PaymentCommand command(String cartId) { return new PaymentCommand(1, UUID.randomUUID().toString(), cartId, PaymentCommand.CREDIT, "4242"); }
    private static PaymentResult approvedResult(String cartId) {
        return new PaymentResult(cartId, PaymentResult.Outcome.APPROVED, java.time.Instant.now(),
            PaymentCommand.CREDIT, "4242", null, new BigDecimal("19.90"), "BRL",
            List.of(new PaymentResult.ProductResult("basico", "Plano Básico", new BigDecimal("19.90"))), null);
    }

    private static final class MapAssertions {
        static void assertNoSensitiveFields(String value) {
            assertFalse(value.toLowerCase().contains("cvv"));
            assertFalse(value.toLowerCase().contains("number"));
            assertFalse(value.contains("4242424242424242"));
        }
    }
}
