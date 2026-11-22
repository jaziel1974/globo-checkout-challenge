package br.com.globo.checkout.worker.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import br.com.globo.checkout.worker.domain.CartSnapshot;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import br.com.globo.checkout.worker.domain.PaymentSimulator;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PaymentProcessorTest {
    @Test void calculatesFromSnapshotAndPersistsTheV1CompatibleResultUnderTheActiveLease() {
        String cartId = UUID.randomUUID().toString();
        String attemptId = UUID.randomUUID().toString();
        var command = new PaymentCommand(1, attemptId, cartId, PaymentCommand.CREDIT, "4242");
        var lease = new PaymentPorts.ProcessingLease(attemptId, "owner-token", System.currentTimeMillis() + 30_000);
        var attempt = new PaymentPorts.AttemptSnapshot(attemptId, cartId, command, "PROCESSANDO", 1, 0, 0, 0, null, null);
        var cart = new CartSnapshot(cartId, "PENDENTE", List.of(
            new CartSnapshot.Product("basico", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL"),
            new CartSnapshot.Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "BRL")),
            List.of(PaymentCommand.CREDIT, PaymentCommand.PIX));

        PaymentPorts.AttemptStore attempts = mock(PaymentPorts.AttemptStore.class);
        PaymentPorts.CartSnapshotReader carts = mock(PaymentPorts.CartSnapshotReader.class);
        when(attempts.find(attemptId)).thenReturn(Optional.of(attempt));
        when(attempts.reserveExecution(lease)).thenReturn(Optional.of(1));
        when(carts.find(cartId)).thenReturn(Optional.of(cart));
        when(attempts.complete(eq(lease), any(PaymentResult.class))).thenReturn(PaymentPorts.CompletionResult.COMPLETED);

        var result = new PaymentProcessor(attempts, carts, new PaymentSimulator()).process(command, lease);
        assertEquals(new BigDecimal("59.80"), result.amount());
        assertEquals(List.of("basico", "premium"), result.products().stream().map(PaymentResult.ProductResult::slug).toList());
        verify(attempts).complete(eq(lease), argThat(stored -> stored.outcome() == PaymentResult.Outcome.APPROVED && stored.lastDigits().equals("4242")));
    }

    @Test void refusesToProcessACommandThatDoesNotMatchItsRecordedAttempt() {
        String cartId = UUID.randomUUID().toString();
        var recorded = new PaymentCommand(1, UUID.randomUUID().toString(), cartId, PaymentCommand.CREDIT, "4242");
        var different = new PaymentCommand(1, recorded.attemptId(), cartId, PaymentCommand.CREDIT, "0002");
        var lease = new PaymentPorts.ProcessingLease(recorded.attemptId(), "owner-token", System.currentTimeMillis() + 30_000);
        var attempt = new PaymentPorts.AttemptSnapshot(recorded.attemptId(), cartId, recorded, "PROCESSANDO", 1, 0, 0, 0, null, null);
        PaymentPorts.AttemptStore attempts = mock(PaymentPorts.AttemptStore.class);
        PaymentPorts.CartSnapshotReader carts = mock(PaymentPorts.CartSnapshotReader.class);
        when(attempts.find(recorded.attemptId())).thenReturn(Optional.of(attempt));
        assertThrows(IllegalStateException.class, () -> new PaymentProcessor(attempts, carts, new PaymentSimulator()).process(different, lease));
        verifyNoInteractions(carts);
        verify(attempts, never()).complete(any(), any());
    }
}
