package br.com.globo.checkout.worker.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class PaymentSimulatorTest {
    private final PaymentSimulator simulator = new PaymentSimulator();
    private final String cartId = UUID.randomUUID().toString();
    private final CartSnapshot cart = new CartSnapshot(cartId, "PENDENTE", List.of(
        new CartSnapshot.Product("basico", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL"),
        new CartSnapshot.Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "BRL")),
        List.of(PaymentCommand.CREDIT, PaymentCommand.DEBIT, PaymentCommand.PIX));

    @Test void approvedCardUsesSnapshotAmountAndReturnsOnlyLastFourDigits() {
        var result = simulator.simulate(command(PaymentCommand.CREDIT, "4242"), cart);
        assertEquals(PaymentResult.Outcome.APPROVED, result.outcome());
        assertEquals(new BigDecimal("59.80"), result.amount());
        assertEquals("BRL", result.currency());
        assertEquals("4242", result.lastDigits());
        assertNull(result.pixCode());
        assertNotNull(result.paidAt());
        assertEquals(List.of("basico", "premium"), result.products().stream().map(PaymentResult.ProductResult::slug).toList());
    }

    @Test void card0002IsDeclinedAndPixReturnsTheSpecifiedDemoCode() throws Exception {
        var declined = simulator.simulate(command(PaymentCommand.DEBIT, "0002"), cart);
        assertEquals(PaymentResult.Outcome.DECLINED, declined.outcome());
        assertEquals("PAYMENT_DECLINED", declined.errorKind());
        assertNull(declined.paidAt());
        String json = new ObjectMapper().writeValueAsString(declined);
        assertFalse(json.toLowerCase().contains("cvv"));
        assertFalse(json.toLowerCase().contains("number"));
        assertFalse(json.contains("4111111111110002"));

        var pix = simulator.simulate(command(PaymentCommand.PIX, null), cart);
        assertEquals(PaymentResult.Outcome.APPROVED, pix.outcome());
        assertNull(pix.lastDigits());
        assertEquals("PIX-SIMULADO-" + cartId, pix.pixCode());
        assertEquals(new BigDecimal("59.80"), pix.amount());
    }

    @Test void rejectsMethodOutsideSnapshotAndAlreadyApprovedCart() {
        var noDebit = new CartSnapshot(cartId, "PENDENTE", cart.products(), List.of(PaymentCommand.CREDIT));
        var methodError = assertThrows(PaymentBusinessException.class, () -> simulator.simulate(command(PaymentCommand.DEBIT, "4242"), noDebit));
        assertEquals(PaymentBusinessException.Kind.PAYMENT_METHOD_NOT_ALLOWED, methodError.kind());

        var approved = new CartSnapshot(cartId, "APROVADO", cart.products(), cart.paymentMethods());
        var approvedError = assertThrows(PaymentBusinessException.class, () -> simulator.simulate(command(PaymentCommand.CREDIT, "4242"), approved));
        assertEquals(PaymentBusinessException.Kind.PAYMENT_ALREADY_APPROVED, approvedError.kind());
    }

    @Test void rejectsMismatchedCartAndMalformedInternalSnapshot() {
        var otherCart = new CartSnapshot(UUID.randomUUID().toString(), "PENDENTE", cart.products(), cart.paymentMethods());
        assertEquals(PaymentBusinessException.Kind.CART_NOT_FOUND,
            assertThrows(PaymentBusinessException.class, () -> simulator.simulate(command(PaymentCommand.CREDIT, "4242"), otherCart)).kind());
        var invalidCurrency = new CartSnapshot(cartId, "PENDENTE", List.of(
            new CartSnapshot.Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "USD")), cart.paymentMethods());
        assertEquals(PaymentBusinessException.Kind.INVALID_REQUEST,
            assertThrows(PaymentBusinessException.class, () -> simulator.simulate(command(PaymentCommand.CREDIT, "4242"), invalidCurrency)).kind());
    }

    private PaymentCommand command(String method, String lastDigits) {
        return new PaymentCommand(1, UUID.randomUUID().toString(), cartId, method, lastDigits);
    }
}
