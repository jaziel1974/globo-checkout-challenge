package br.com.globo.checkout.worker.domain;

import java.math.BigDecimal;
import java.util.List;

public record CartSnapshot(String cartId, String status, List<Product> products,
                           List<String> paymentMethods) {
    public record Product(String slug, String name, String description,
                          BigDecimal price, String currency) {}
}
