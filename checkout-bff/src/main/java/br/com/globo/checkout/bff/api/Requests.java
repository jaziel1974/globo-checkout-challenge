package br.com.globo.checkout.bff.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import tools.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

public final class Requests {
    private Requests() {}
    public record PreCheckoutRequest(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) @NotEmpty List<@Valid ProductSelection> products) {}
    public record ProductSelection(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) @NotBlank String slug) {}
    public record CheckoutRequest(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) @NotBlank String cartId, @Schema(requiredMode = Schema.RequiredMode.REQUIRED) @Valid PaymentSelection paymentMethod) {}
    public record PaymentSelection(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) @NotBlank String name, @Schema(nullable = true) @Valid Card card) {}
    public record Card(
        @Schema(requiredMode = Schema.RequiredMode.REQUIRED) @NotBlank @JsonDeserialize(using = StrictStringDeserializer.class) String holderName,
        @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Somente digitos; minimo de quatro digitos") @NotBlank @JsonDeserialize(using = StrictStringDeserializer.class) String number,
        @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "12/28", pattern = "(0[1-9]|1[0-2])/[0-9]{2}") @NotBlank @JsonDeserialize(using = StrictStringDeserializer.class) String expiry,
        @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Somente digitos") @NotBlank @JsonDeserialize(using = StrictStringDeserializer.class) String cvv
    ) {}
}
