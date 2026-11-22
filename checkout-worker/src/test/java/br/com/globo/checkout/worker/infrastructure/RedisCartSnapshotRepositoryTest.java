package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import br.com.globo.checkout.bff.domain.Models;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

class RedisCartSnapshotRepositoryTest {
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test void readsJavaSerializedV1PayloadAndCurrentStatusWithoutWritingV1State() throws Exception {
        String cartId = "aef6fba0-8773-4e1f-a84d-b8f85e4b48af";
        var cart = new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "BRL")),
            List.of(new Models.PaymentMethod("Cartão de Crédito"), new Models.PaymentMethod("Pix")));
        String payload = serialize(cart);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(hashes.get("checkout:cart:" + cartId, "payload")).thenReturn(payload);
        when(hashes.get("checkout:cart:" + cartId, "status")).thenReturn("APROVADO");

        var snapshot = new RedisCartSnapshotRepository(redis).find(cartId).orElseThrow();
        assertEquals("APROVADO", snapshot.status());
        assertEquals("39.90", snapshot.products().getFirst().price().toPlainString());
        assertEquals(List.of("Cartão de Crédito", "Pix"), snapshot.paymentMethods());
        verify(hashes, never()).put(any(), any(), any());
        verify(hashes, never()).putAll(any(), anyMap());
    }

    private static String serialize(Models.Cart cart) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject(cart); }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }
}
