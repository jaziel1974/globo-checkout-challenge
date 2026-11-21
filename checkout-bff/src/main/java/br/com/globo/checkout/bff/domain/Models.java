package br.com.globo.checkout.bff.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

public final class Models {
    private Models() {}
    public record Product(String slug, String name, String description, BigDecimal price, String currency) implements Serializable {}
    public record PaymentMethod(String name) implements Serializable {}
    public record Cart(String cartId, String status, List<Product> products, List<PaymentMethod> paymentMethods) implements Serializable {
        public Cart withStatus(String next) { return new Cart(cartId, next, products, paymentMethods); }
    }
}
