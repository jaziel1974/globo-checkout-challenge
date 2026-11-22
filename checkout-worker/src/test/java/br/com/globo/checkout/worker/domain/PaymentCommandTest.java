package br.com.globo.checkout.worker.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class PaymentCommandTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test void serializesMinimalCardAndPixCommands() throws Exception {
        var card = new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(), PaymentCommand.CREDIT, "4242");
        String json = mapper.writeValueAsString(card);
        assertEquals(card, mapper.readValue(json, PaymentCommand.class));
        assertFalse(json.toLowerCase().contains("cvv"));
        assertFalse(json.toLowerCase().contains("number"));
        assertFalse(json.contains("4242424242424242"));

        var pix = new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(), PaymentCommand.PIX, null);
        assertEquals(pix, mapper.readValue(mapper.writeValueAsString(pix), PaymentCommand.class));
    }

    @Test void rejectsMalformedAndUnknownCommands() {
        assertThrows(IllegalArgumentException.class, () -> new PaymentCommand(2, UUID.randomUUID().toString(), UUID.randomUUID().toString(), PaymentCommand.PIX, null));
        assertThrows(IllegalArgumentException.class, () -> new PaymentCommand(1, "not-a-uuid", UUID.randomUUID().toString(), PaymentCommand.PIX, null));
        assertThrows(IllegalArgumentException.class, () -> new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(), PaymentCommand.CREDIT, "12x4"));
        assertThrows(IllegalArgumentException.class, () -> new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(), "unknown", null));
    }

    @Test void pixKafkaWireFormOmitsLastDigitsProperty() {
        var pix = new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(), PaymentCommand.PIX, null);
        assertEquals(java.util.Set.of("schemaVersion", "attemptId", "cartId", "paymentMethod"), pix.toWireFields().keySet());
        assertFalse(pix.toWireFields().containsKey("lastDigits"));
    }
}
