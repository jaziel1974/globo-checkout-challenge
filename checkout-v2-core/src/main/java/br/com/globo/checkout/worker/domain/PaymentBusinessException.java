package br.com.globo.checkout.worker.domain;

public final class PaymentBusinessException extends RuntimeException {
    public enum Kind { CART_NOT_FOUND, PAYMENT_ALREADY_APPROVED, PAYMENT_METHOD_NOT_ALLOWED, INVALID_REQUEST }
    private final Kind kind;

    public PaymentBusinessException(Kind kind) {
        super(kind.name());
        this.kind = kind;
    }

    public Kind kind() { return kind; }
}
