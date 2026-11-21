package br.com.globo.checkout.bff.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public final class Responses {
    private Responses() {}
    public record PreCheckoutResponse(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String cartId,
                                      @Schema(requiredMode = Schema.RequiredMode.REQUIRED) List<ProductView> products,
                                      @Schema(requiredMode = Schema.RequiredMode.REQUIRED) List<PaymentMethodView> paymentMethods) {}
    public record ProductView(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String slug,
                              @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String name,
                              @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String description,
                              @Schema(requiredMode = Schema.RequiredMode.REQUIRED, type = "number", example = "39.90") BigDecimal price,
                              @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "BRL") String currency) {}
    public record CheckoutProductView(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String slug,
                                      @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String name,
                                      @Schema(requiredMode = Schema.RequiredMode.REQUIRED, type = "number", example = "39.90") BigDecimal price) {}
    public record PaymentMethodView(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String name) {}
    public record PaymentView(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String method,
                              @JsonInclude(NON_NULL) @Schema(nullable = true, description = "Presente para pagamento com cartao") String lastDigits,
                              @JsonInclude(NON_NULL) @Schema(nullable = true, description = "Presente para Pix") String pixCode,
                              @Schema(requiredMode = Schema.RequiredMode.REQUIRED, type = "number", example = "39.90") BigDecimal amount,
                              @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "BRL") String currency) {}
    public record CheckoutResponse(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String cartId,
                                   @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "APROVADO") String status,
                                   @Schema(requiredMode = Schema.RequiredMode.REQUIRED, format = "date-time", example = "2026-10-14T16:00:00Z") Instant paidAt,
                                   @Schema(requiredMode = Schema.RequiredMode.REQUIRED) PaymentView payment,
                                   @Schema(requiredMode = Schema.RequiredMode.REQUIRED) List<CheckoutProductView> products) {}
}
