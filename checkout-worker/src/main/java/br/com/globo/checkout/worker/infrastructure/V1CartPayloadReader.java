package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.worker.domain.CartSnapshot;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.List;

/** Reads only the Base64 Java-serialized payload written by the unchanged v1 repository. */
public final class V1CartPayloadReader {
    private static final ObjectInputFilter FILTER = info -> {
        if (info.depth() > 8 || info.references() > 128 || info.streamBytes() > 1_048_576 || info.arrayLength() > 256) {
            return ObjectInputFilter.Status.REJECTED;
        }
        Class<?> type = info.serialClass();
        if (type == null) return ObjectInputFilter.Status.UNDECIDED;
        if (type.isArray()) return type == Object[].class || type == byte[].class || type == int[].class
            ? ObjectInputFilter.Status.ALLOWED : ObjectInputFilter.Status.REJECTED;
        String name = type.getName();
        return switch (name) {
            case "br.com.globo.checkout.bff.domain.Models$Cart",
                 "br.com.globo.checkout.bff.domain.Models$Product",
                 "br.com.globo.checkout.bff.domain.Models$PaymentMethod",
                 "java.lang.String", "java.lang.Number", "java.math.BigDecimal", "java.math.BigInteger",
                 "java.util.CollSer", "java.util.ImmutableCollections$List12",
                 "java.util.ImmutableCollections$ListN" -> ObjectInputFilter.Status.ALLOWED;
            default -> ObjectInputFilter.Status.REJECTED;
        };
    };

    public CartSnapshot read(String base64Payload) {
        if (base64Payload == null || base64Payload.isBlank()) throw new IllegalArgumentException("Cart payload is required");
        try (var bytes = new ByteArrayInputStream(Base64.getDecoder().decode(base64Payload));
             var input = new ObjectInputStream(bytes)) {
            input.setObjectInputFilter(FILTER);
            Object decoded = input.readObject();
            if (!(decoded instanceof Models.Cart cart)) throw new IllegalArgumentException("Invalid cart payload");
            return new CartSnapshot(cart.cartId(), cart.status(),
                cart.products().stream().map(p -> new CartSnapshot.Product(p.slug(), p.name(), p.description(), p.price(), p.currency())).toList(),
                cart.paymentMethods().stream().map(Models.PaymentMethod::name).toList());
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid cart payload", ex);
        }
    }
}
