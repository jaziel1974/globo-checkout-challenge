package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import br.com.globo.checkout.worker.application.PaymentCommandHandler;
import br.com.globo.checkout.worker.application.PaymentPorts;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class KafkaCommandListenerTest {
    @Test void failedDltForwardingRemainsPendingAndDoesNotRerunPaymentAfterRedisRecovers() {
        var handler = mock(PaymentCommandHandler.class);
        var failures = mock(PaymentPorts.FailureQueuePublisher.class);
        var notice = new PaymentPorts.FailureNotice("attempt", "cart", "commands", 0, 7, "REDIS_UNAVAILABLE");
        when(handler.handleDetailed("cart", "safe-minimal-wire")).thenReturn(new PaymentCommandHandler.HandlingResult(
            PaymentCommandHandler.Outcome.REDIS_UNAVAILABLE, "attempt", "cart", "REDIS_UNAVAILABLE"));
        when(handler.resolveRedisFailure("cart", "safe-minimal-wire")).thenReturn(new PaymentCommandHandler.RedisFailureAttempt(
            PaymentCommandHandler.RedisFailureResolution.PERSISTED, "attempt", "cart", "REDIS_UNAVAILABLE"));
        when(failures.publish(any(), eq(Duration.ofSeconds(2)))).thenReturn(false, true);
        var listener = new KafkaCommandListener(handler, failures);
        var firstAck = mock(Acknowledgment.class);
        var secondAck = mock(Acknowledgment.class);
        var thirdAck = mock(Acknowledgment.class);
        var fourthAck = mock(Acknowledgment.class);
        var record = new ConsumerRecord<>("commands", 0, 7, "cart", "safe-minimal-wire");

        listener.consume(record, firstAck);
        verify(firstAck).nack(Duration.ofSeconds(1));
        listener.consume(record, secondAck);
        verify(secondAck).nack(Duration.ofSeconds(1));
        listener.consume(record, thirdAck);
        verify(thirdAck).nack(Duration.ofSeconds(1));
        listener.consume(record, fourthAck);

        verify(handler, times(3)).handleDetailed("cart", "safe-minimal-wire");
        verify(failures, times(2)).publish(any(), eq(Duration.ofSeconds(2)));
        verify(fourthAck).acknowledge();
        verify(fourthAck, never()).nack(any());
    }

    @Test void unknownPayloadIsOnlySentAsSafeRoutingMetadata() {
        var handler = mock(PaymentCommandHandler.class);
        var failures = mock(PaymentPorts.FailureQueuePublisher.class);
        when(handler.handleDetailed("cart", "PAN=4111111111111111"))
            .thenReturn(new PaymentCommandHandler.HandlingResult(PaymentCommandHandler.Outcome.INVALID, null, null, "INVALID_COMMAND"));
        when(failures.publish(any(), any())).thenReturn(true);
        var listener = new KafkaCommandListener(handler, failures);
        var ack = mock(Acknowledgment.class);
        listener.consume(new ConsumerRecord<>("commands", 2, 11, "cart", "PAN=4111111111111111"), ack);
        var captured = org.mockito.ArgumentCaptor.forClass(PaymentPorts.FailureNotice.class);
        verify(failures).publish(captured.capture(), any());
        assertEquals("commands", captured.getValue().sourceTopic());
        assertEquals(2, captured.getValue().sourcePartition());
        assertEquals(11, captured.getValue().sourceOffset());
        assertEquals("INVALID_COMMAND", captured.getValue().reasonCode());
        assertNull(captured.getValue().attemptId());
        assertNull(captured.getValue().cartId());
        verify(ack).acknowledge();
    }

    @Test void incompleteTerminalStateIsForwardedSafelyAndNeverAcknowledgedAsSuccess() {
        var handler = mock(PaymentCommandHandler.class);
        var failures = mock(PaymentPorts.FailureQueuePublisher.class);
        String attemptId = "attempt";
        String cartId = "cart";
        when(handler.handleDetailed(cartId, "safe-minimal-wire")).thenReturn(new PaymentCommandHandler.HandlingResult(
            PaymentCommandHandler.Outcome.REDIS_UNAVAILABLE, attemptId, cartId, "REDIS_UNAVAILABLE"));
        when(handler.resolveRedisFailure(cartId, "safe-minimal-wire")).thenReturn(new PaymentCommandHandler.RedisFailureAttempt(
            PaymentCommandHandler.RedisFailureResolution.MISSING_STATE, attemptId, cartId, "INCOMPLETE_TERMINAL_STATE"));
        when(failures.publish(any(), any())).thenReturn(true);
        var listener = new KafkaCommandListener(handler, failures);
        var firstAck = mock(Acknowledgment.class);
        var secondAck = mock(Acknowledgment.class);
        var thirdAck = mock(Acknowledgment.class);
        var record = new ConsumerRecord<>("commands", 0, 9, cartId, "safe-minimal-wire");

        listener.consume(record, firstAck);
        listener.consume(record, secondAck);
        listener.consume(record, thirdAck);

        var captured = org.mockito.ArgumentCaptor.forClass(PaymentPorts.FailureNotice.class);
        verify(failures).publish(captured.capture(), eq(Duration.ofSeconds(2)));
        assertEquals("INCOMPLETE_TERMINAL_STATE", captured.getValue().reasonCode());
        verify(firstAck).nack(Duration.ofSeconds(1));
        verify(secondAck).nack(Duration.ofSeconds(1));
        verify(thirdAck).acknowledge();
        verify(thirdAck, never()).nack(any());
        verify(handler, times(3)).handleDetailed(cartId, "safe-minimal-wire");
    }
}
