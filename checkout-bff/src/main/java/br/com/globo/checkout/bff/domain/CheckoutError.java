package br.com.globo.checkout.bff.domain;

public class CheckoutError extends RuntimeException {
    public enum Kind { INVALID_REQUEST, PAYMENT_METHOD_NOT_ALLOWED, PRODUCT_NOT_FOUND, CART_NOT_FOUND, PAYMENT_ALREADY_APPROVED, PAYMENT_DECLINED, DEPENDENCY_UNAVAILABLE, UNKNOWN_APPROVAL }
    private final Kind kind;
    private final String cartId;
    public CheckoutError(Kind kind, String cartId) { super(kind.name()); this.kind = kind; this.cartId = cartId; }
    public CheckoutError(Kind kind, String cartId, Throwable cause) { super(kind.name(), cause); this.kind = kind; this.cartId = cartId; }
    public Kind kind() { return kind; }
    public String cartId() { return cartId; }
}
