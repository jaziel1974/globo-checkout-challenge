package br.com.globo.checkout.worker.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Internal result data required to reproduce the existing v1 checkout response. */
public record PaymentResult(String cartId, Outcome outcome, Instant paidAt,
                            String paymentMethod, String lastDigits, String pixCode,
                            BigDecimal amount, String currency, List<ProductResult> products,
                            String errorKind) {
    public enum Outcome { APPROVED, DECLINED }
    public record ProductResult(String slug, String name, BigDecimal price) {}
}
