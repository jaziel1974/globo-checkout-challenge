package br.com.globo.checkout.worker.domain;

import java.util.UUID;
import java.util.LinkedHashMap;
import java.util.Map;

/** Minimal, versioned Kafka command. It deliberately has no PAN or CVV fields. */
public record PaymentCommand(int schemaVersion, String attemptId, String cartId,
                             String paymentMethod, String lastDigits) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String PIX = "Pix";
    public static final String CREDIT = "Cartão de Crédito";
    public static final String DEBIT = "Cartão de Débito";

    public PaymentCommand {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) throw new IllegalArgumentException("Unsupported schemaVersion");
        requireUuid(attemptId, "attemptId");
        requireUuid(cartId, "cartId");
        if (paymentMethod == null || paymentMethod.isBlank()) throw new IllegalArgumentException("paymentMethod is required");
        if (PIX.equals(paymentMethod)) {
            if (lastDigits != null) throw new IllegalArgumentException("Pix command must not include lastDigits");
        } else if (CREDIT.equals(paymentMethod) || DEBIT.equals(paymentMethod)) {
            if (lastDigits == null || !lastDigits.matches("\\d{4}")) throw new IllegalArgumentException("Card command requires four lastDigits");
        } else {
            throw new IllegalArgumentException("Unsupported paymentMethod");
        }
    }

    private static void requireUuid(String value, String field) {
        if (value == null) throw new IllegalArgumentException(field + " must be a UUID");
        try {
            if (!UUID.fromString(value).toString().equalsIgnoreCase(value)) throw new IllegalArgumentException(field + " must be a UUID");
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(field + " must be a UUID");
        }
    }

    /** Wire form omits lastDigits entirely for Pix; card numbers and CVV are never represented. */
    public Map<String, Object> toWireFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("schemaVersion", schemaVersion);
        fields.put("attemptId", attemptId);
        fields.put("cartId", cartId);
        fields.put("paymentMethod", paymentMethod);
        if (lastDigits != null) fields.put("lastDigits", lastDigits);
        return Map.copyOf(fields);
    }
}
