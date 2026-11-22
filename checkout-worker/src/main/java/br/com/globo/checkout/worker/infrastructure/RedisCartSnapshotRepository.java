package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.CartSnapshot;
import java.util.Optional;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/** Reads the v1 snapshot and its current status without writing to v1 Redis keys. */
@Repository
public class RedisCartSnapshotRepository implements PaymentPorts.CartSnapshotReader {
    private final StringRedisTemplate redis;
    private final V1CartPayloadReader payloadReader;

    public RedisCartSnapshotRepository(StringRedisTemplate redis) {
        this.redis = redis;
        this.payloadReader = new V1CartPayloadReader();
    }

    @Override public Optional<CartSnapshot> find(String cartId) {
        String key = "checkout:cart:" + cartId;
        Object rawPayload = redis.opsForHash().get(key, "payload");
        if (!(rawPayload instanceof String payload)) return Optional.empty();
        Object rawStatus = redis.opsForHash().get(key, "status");
        if (!(rawStatus instanceof String status)) throw new IllegalStateException("Invalid cart state");
        CartSnapshot payloadCart = payloadReader.read(payload);
        if (!cartId.equals(payloadCart.cartId())) throw new IllegalStateException("Cart payload identifier mismatch");
        return Optional.of(new CartSnapshot(payloadCart.cartId(), status, payloadCart.products(), payloadCart.paymentMethods()));
    }
}
