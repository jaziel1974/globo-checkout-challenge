package br.com.globo.checkout.bff.infrastructure;

import br.com.globo.checkout.bff.domain.Models.Cart;
import br.com.globo.checkout.bff.domain.Ports;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

@Repository
public class RedisCartRepository implements Ports.CartRepository {
    private static final String PREFIX = "checkout:cart:";
    private static final DefaultRedisScript<Long> APPROVE = new DefaultRedisScript<>(
        "local status = redis.call('HGET', KEYS[1], 'status') " +
        "if not status then return 0 end " +
        "if status == 'APROVADO' then return 2 end " +
        "redis.call('HSET', KEYS[1], 'status', 'APROVADO') return 1", Long.class);
    private final StringRedisTemplate redis;
    public RedisCartRepository(StringRedisTemplate redis) { this.redis = redis; }

    @Override public void create(Cart cart) {
        String key = key(cart.cartId());
        redis.opsForHash().putAll(key, java.util.Map.of("payload", encode(cart), "status", cart.status()));
    }
    @Override public Optional<Cart> find(String cartId) {
        String key = key(cartId);
        Object payload = redis.opsForHash().get(key, "payload");
        if (payload == null) return Optional.empty();
        Object status = redis.opsForHash().get(key, "status");
        if (!(payload instanceof String value) || !(status instanceof String state)) throw new IllegalStateException("Invalid cart state");
        return Optional.of(decode(value).withStatus(state));
    }
    @Override public Ports.ApprovalResult approve(String cartId) {
        Long result = redis.execute(APPROVE, List.of(key(cartId)));
        if (result == null) throw new IllegalStateException("Redis approval returned no result");
        return switch (result.intValue()) {
            case 0 -> Ports.ApprovalResult.NOT_FOUND;
            case 1 -> Ports.ApprovalResult.APPROVED;
            case 2 -> Ports.ApprovalResult.ALREADY_APPROVED;
            default -> throw new IllegalStateException("Invalid Redis approval result");
        };
    }
    private String key(String cartId) { return PREFIX + cartId; }
    private String encode(Cart cart) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(cart); return Base64.getEncoder().encodeToString(bytes.toByteArray());
        } catch (Exception e) { throw new IllegalStateException("Could not encode cart", e); }
    }
    private Cart decode(String value) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(value)))) {
            Object object = in.readObject();
            if (!(object instanceof Cart cart)) throw new IllegalStateException("Invalid cart payload");
            return cart;
        } catch (Exception e) { throw new IllegalStateException("Could not decode cart", e); }
    }
}
