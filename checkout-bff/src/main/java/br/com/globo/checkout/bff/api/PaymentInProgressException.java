package br.com.globo.checkout.bff.api;

public final class PaymentInProgressException extends RuntimeException {
    private final String cartId;
    public PaymentInProgressException(String cartId) { super("PAYMENT_IN_PROGRESS"); this.cartId = cartId; }
    public String cartId() { return cartId; }
}
