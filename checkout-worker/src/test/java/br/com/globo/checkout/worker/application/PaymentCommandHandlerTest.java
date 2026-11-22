package br.com.globo.checkout.worker.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class PaymentCommandHandlerTest {
    @Test void acceptsMinimalPixRecordWithoutLastDigits() {
        var attempts = mock(PaymentPorts.AttemptStore.class);
        when(attempts.find("0a5d91ab-bad6-4190-8953-3cf84a6d43ea")).thenReturn(Optional.empty());
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), attempts, mock(PaymentProcessor.class),
                new ScheduledLeaseRenewer(attempts, scheduler, 10_000));
            String value = "{\"schemaVersion\":1,\"attemptId\":\"0a5d91ab-bad6-4190-8953-3cf84a6d43ea\","
                + "\"cartId\":\"e9f0a880-99f5-477d-99b7-58b73f0fbb28\",\"paymentMethod\":\"Pix\"}";
            assertEquals(PaymentCommandHandler.Outcome.MISSING_STATE,
                handler.handle("e9f0a880-99f5-477d-99b7-58b73f0fbb28", value));
        } finally { scheduler.shutdownNow(); }
    }

    @Test void refusesToAcknowledgeTerminalStateWithoutARecordedResult() {
        String attemptId = UUID.randomUUID().toString();
        String cartId = UUID.randomUUID().toString();
        var command = new br.com.globo.checkout.worker.domain.PaymentCommand(1, attemptId, cartId,
            br.com.globo.checkout.worker.domain.PaymentCommand.PIX, null);
        var attempts = mock(PaymentPorts.AttemptStore.class);
        when(attempts.find(attemptId)).thenReturn(Optional.of(new PaymentPorts.AttemptSnapshot(
            attemptId, cartId, command, "APROVADO", 1, 1, 0, 0, null, null)));
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), attempts, mock(PaymentProcessor.class),
                new ScheduledLeaseRenewer(attempts, scheduler, 10_000));
            String wire = "{\"schemaVersion\":1,\"attemptId\":\"" + attemptId + "\",\"cartId\":\"" + cartId
                + "\",\"paymentMethod\":\"Pix\"}";
            assertEquals(PaymentCommandHandler.Outcome.MISSING_STATE, handler.handle(cartId, wire));
            verify(attempts, never()).acquire(anyString());
        } finally { scheduler.shutdownNow(); }
    }

    @Test void rejectsPixWithLastDigitsOrAnyUnknownFieldBeforeReadingRedis() {
        var attempts = mock(PaymentPorts.AttemptStore.class);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), attempts, mock(PaymentProcessor.class),
                new ScheduledLeaseRenewer(attempts, scheduler, 10_000));
            String withLastDigits = "{\"schemaVersion\":1,\"attemptId\":\"0a5d91ab-bad6-4190-8953-3cf84a6d43ea\","
                + "\"cartId\":\"e9f0a880-99f5-477d-99b7-58b73f0fbb28\",\"paymentMethod\":\"Pix\",\"lastDigits\":null}";
            String withUnknown = "{\"schemaVersion\":1,\"attemptId\":\"0a5d91ab-bad6-4190-8953-3cf84a6d43ea\","
                + "\"cartId\":\"e9f0a880-99f5-477d-99b7-58b73f0fbb28\",\"paymentMethod\":\"Pix\",\"pan\":\"forbidden\"}";
            assertEquals(PaymentCommandHandler.Outcome.INVALID, handler.handle("e9f0a880-99f5-477d-99b7-58b73f0fbb28", withLastDigits));
            assertEquals(PaymentCommandHandler.Outcome.INVALID, handler.handle("e9f0a880-99f5-477d-99b7-58b73f0fbb28", withUnknown));
            verifyNoInteractions(attempts);
        } finally { scheduler.shutdownNow(); }
    }

    @Test void outageResolutionPersistsInterventionBeforeDltAndPreservesTerminalOrMissingState() {
        String attemptId = "0a5d91ab-bad6-4190-8953-3cf84a6d43ea";
        String cartId = "e9f0a880-99f5-477d-99b7-58b73f0fbb28";
        var command = new br.com.globo.checkout.worker.domain.PaymentCommand(1, attemptId, cartId,
            br.com.globo.checkout.worker.domain.PaymentCommand.PIX, null);
        String wire = "{\"schemaVersion\":1,\"attemptId\":\"" + attemptId + "\",\"cartId\":\"" + cartId
            + "\",\"paymentMethod\":\"Pix\"}";
        var attempts = mock(PaymentPorts.AttemptStore.class);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var handler = new PaymentCommandHandler(new ObjectMapper(), attempts, mock(PaymentProcessor.class),
                new ScheduledLeaseRenewer(attempts, scheduler, 10_000));
            when(attempts.find(attemptId)).thenReturn(Optional.of(new PaymentPorts.AttemptSnapshot(
                attemptId, cartId, command, "PUBLICADO", 1, 0, 0, 0, null, null)));
            when(attempts.markIntervention(attemptId, "REDIS_UNAVAILABLE")).thenReturn(true);
            var persisted = handler.resolveRedisFailure(cartId, wire);
            assertEquals(PaymentCommandHandler.RedisFailureResolution.PERSISTED, persisted.resolution());
            verify(attempts).markIntervention(attemptId, "REDIS_UNAVAILABLE");

            when(attempts.find(attemptId)).thenReturn(Optional.empty());
            assertEquals(PaymentCommandHandler.RedisFailureResolution.MISSING_STATE,
                handler.resolveRedisFailure(cartId, wire).resolution());
            when(attempts.find(attemptId)).thenReturn(Optional.of(new PaymentPorts.AttemptSnapshot(
                attemptId, cartId, command, "APROVADO", 1, 1, 0, 0, null,
                null)));
            assertEquals(PaymentCommandHandler.RedisFailureResolution.MISSING_STATE,
                handler.resolveRedisFailure(cartId, wire).resolution(), "incomplete terminal state is forwarded safely, never acknowledged");
            when(attempts.find(attemptId)).thenReturn(Optional.of(new PaymentPorts.AttemptSnapshot(
                attemptId, cartId, command, "APROVADO", 1, 1, 0, 0, null,
                new br.com.globo.checkout.worker.domain.PaymentResult(cartId,
                    br.com.globo.checkout.worker.domain.PaymentResult.Outcome.APPROVED, java.time.Instant.now(), "Pix",
                    null, null, java.math.BigDecimal.ONE, "BRL", java.util.List.of(), null))));
            assertEquals(PaymentCommandHandler.RedisFailureResolution.TERMINAL,
                handler.resolveRedisFailure(cartId, wire).resolution());
            assertEquals(PaymentCommandHandler.RedisFailureResolution.MISMATCH,
                handler.resolveRedisFailure("another-cart", wire).resolution());
        } finally { scheduler.shutdownNow(); }
    }
}
