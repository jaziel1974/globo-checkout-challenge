package br.com.globo.checkout.bff.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import br.com.globo.checkout.bff.api.Requests.*;
import br.com.globo.checkout.bff.api.Responses.CheckoutResponse;
import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.Models.*;
import br.com.globo.checkout.bff.domain.Ports;
import br.com.globo.checkout.bff.domain.Ports.ApprovalResult;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CheckoutServiceTest {
    private Ports.ProductCatalog productCatalog; private Ports.PaymentMethodCatalog paymentCatalog; private Ports.CartRepository carts; private CheckoutService service;
    private final Product premium = new Product("premium", "Plano Premium", "Assinatura mensal com catálogo completo", new BigDecimal("39.90"), "BRL");
    private final List<PaymentMethod> methods = List.of(new PaymentMethod("Cartão de Crédito"), new PaymentMethod("Pix"));
    @BeforeEach void setUp() { productCatalog = mock(Ports.ProductCatalog.class); paymentCatalog = mock(Ports.PaymentMethodCatalog.class); carts = mock(Ports.CartRepository.class); service = new CheckoutService(productCatalog, paymentCatalog, carts); }
    @Test void preCheckoutStoresSnapshotAndReturnsContract() {
        when(productCatalog.getProduct("premium")).thenReturn(premium); when(paymentCatalog.getPaymentMethods()).thenReturn(methods);
        var response = service.preCheckout(new PreCheckoutRequest(List.of(new ProductSelection("premium"))));
        assertEquals(List.of("premium"), response.products().stream().map(p -> p.slug()).toList()); assertEquals("BRL", response.products().getFirst().currency()); assertEquals(2, response.paymentMethods().size());
        verify(carts).create(argThat(cart -> "PENDENTE".equals(cart.status()) && cart.products().equals(List.of(premium))));
    }
    @Test void rejectsEmptyAndRepeatedProductsWithoutCreatingCart() {
        assertEquals(CheckoutError.Kind.INVALID_REQUEST, assertThrows(CheckoutError.class, () -> service.preCheckout(new PreCheckoutRequest(List.of()))).kind());
        assertEquals(CheckoutError.Kind.INVALID_REQUEST, assertThrows(CheckoutError.class, () -> service.preCheckout(new PreCheckoutRequest(List.of(new ProductSelection("premium"), new ProductSelection("premium"))))).kind()); verifyNoInteractions(carts);
    }
    @Test void pixApprovalUsesExactSnapshotValueAndPixCode() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1"))); when(carts.approve("c1")).thenReturn(ApprovalResult.APPROVED);
        CheckoutResponse response = service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null)));
        assertEquals(new BigDecimal("39.90"), response.payment().amount()); assertEquals("PIX-SIMULADO-c1", response.payment().pixCode()); assertNull(response.payment().lastDigits()); assertEquals("APROVADO", response.status());
    }
    @Test void declinedCardDoesNotApproveAndCanBeRetried() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1")));
        CheckoutError declined = assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Cartão de Crédito", new Card("Ana", "4111111111110002", "12/28", "123")))));
        assertEquals(CheckoutError.Kind.PAYMENT_DECLINED, declined.kind()); verify(carts, never()).approve(any());
        when(carts.approve("c1")).thenReturn(ApprovalResult.APPROVED);
        assertEquals("4242", service.checkout(new CheckoutRequest("c1", new PaymentSelection("Cartão de Crédito", new Card("Ana", "4111111111114242", "12/28", "123")))).payment().lastDigits());
    }
    @Test void rejectsPaymentMethodNotInSnapshotAndAlreadyApprovedCart() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1")));
        assertEquals(CheckoutError.Kind.PAYMENT_METHOD_NOT_ALLOWED, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Cartão de Débito", new Card("Ana", "4111111111114242", "12/28", "123"))))).kind());
        when(carts.approve("c1")).thenReturn(ApprovalResult.ALREADY_APPROVED);
        assertEquals(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null)))).kind());
    }
    @Test void approvedCartAlwaysWinsOverDeclinedCardRetry() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1").withStatus("APROVADO")));
        CheckoutError error = assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Cartão de Crédito", new Card("Ana", "4111111111110002", "12/28", "123")))));
        assertEquals(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, error.kind());
        assertEquals("c1", error.cartId());
        verify(carts, never()).approve(any());
    }
    @Test void neverAcceptsCardDataForPixOrMalformedCard() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1")));
        assertEquals(CheckoutError.Kind.INVALID_REQUEST, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", new Card("Ana", "4111", "12/28", "123"))))).kind());
        assertEquals(CheckoutError.Kind.INVALID_REQUEST, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Cartão de Crédito", new Card("Ana", "41x1", "13/28", "123"))))).kind());
    }
    @Test void reportsUnknownResultWhenRedisApprovalThrows() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1"))); when(carts.approve("c1")).thenThrow(new IllegalStateException("connection lost"));
        CheckoutError error = assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null))));
        assertEquals(CheckoutError.Kind.UNKNOWN_APPROVAL, error.kind()); assertEquals("c1", error.cartId());
    }
    @Test void retryAfterUnknownResultReadsStateAndDoesNotApproveTwice() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1")), Optional.of(cart("c1").withStatus("APROVADO")));
        when(carts.approve("c1")).thenThrow(new IllegalStateException("connection lost")).thenReturn(ApprovalResult.ALREADY_APPROVED);
        assertEquals(CheckoutError.Kind.UNKNOWN_APPROVAL, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null)))).kind());
        assertEquals(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null)))).kind());
        verify(carts, times(1)).approve("c1");
    }
    @Test void retryAfterUnknownUnappliedResultCanApproveOnce() {
        when(carts.find("c1")).thenReturn(Optional.of(cart("c1")), Optional.of(cart("c1")));
        when(carts.approve("c1")).thenThrow(new IllegalStateException("connection lost")).thenReturn(ApprovalResult.APPROVED);
        assertEquals(CheckoutError.Kind.UNKNOWN_APPROVAL, assertThrows(CheckoutError.class, () -> service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null)))).kind());
        assertEquals("APROVADO", service.checkout(new CheckoutRequest("c1", new PaymentSelection("Pix", null))).status());
        verify(carts, times(2)).approve("c1");
    }
    private Cart cart(String id) { return new Cart(id, "PENDENTE", List.of(premium), methods); }
}
