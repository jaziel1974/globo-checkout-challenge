package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;

import br.com.globo.checkout.bff.domain.Models;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

class V1CartPayloadReaderTest {
    private final V1CartPayloadReader reader = new V1CartPayloadReader();

    @Test void readsOnlyTheV1SerializedCartIntoTheWorkerModel() throws Exception {
        var cart = new Models.Cart("bff-cart", "PENDENTE",
            List.of(new Models.Product("basico", "Plano Básico", "Assinatura", new BigDecimal("19.90"), "BRL"),
                    new Models.Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "BRL"),
                    new Models.Product("familia", "Plano Família", "Assinatura", new BigDecimal("59.90"), "BRL")),
            List.of(new Models.PaymentMethod("Pix")));
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject(cart); }
        var snapshot = reader.read(Base64.getEncoder().encodeToString(bytes.toByteArray()));
        assertEquals("bff-cart", snapshot.cartId());
        assertEquals(List.of(new BigDecimal("19.90"), new BigDecimal("39.90"), new BigDecimal("59.90")),
            snapshot.products().stream().map(p -> p.price()).toList());
        assertEquals(List.of("Pix"), snapshot.paymentMethods());
    }

    @Test void rejectsArbitrarySerializedObjects() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject("not a cart"); }
        assertThrows(IllegalArgumentException.class, () -> reader.read(Base64.getEncoder().encodeToString(bytes.toByteArray())));
    }
}
