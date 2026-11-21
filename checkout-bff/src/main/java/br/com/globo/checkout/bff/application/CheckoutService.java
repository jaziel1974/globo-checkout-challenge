package br.com.globo.checkout.bff.application;

import br.com.globo.checkout.bff.api.Requests.Card;
import br.com.globo.checkout.bff.api.Requests.CheckoutRequest;
import br.com.globo.checkout.bff.api.Requests.PreCheckoutRequest;
import br.com.globo.checkout.bff.api.Responses.*;
import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.CheckoutError.Kind;
import br.com.globo.checkout.bff.domain.Models.Cart;
import br.com.globo.checkout.bff.domain.Models.PaymentMethod;
import br.com.globo.checkout.bff.domain.Models.Product;
import br.com.globo.checkout.bff.domain.Ports;
import br.com.globo.checkout.bff.domain.Ports.ApprovalResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CheckoutService {
    private static final Logger log = LoggerFactory.getLogger(CheckoutService.class);
    private final Ports.ProductCatalog products;
    private final Ports.PaymentMethodCatalog methods;
    private final Ports.CartRepository carts;

    public CheckoutService(Ports.ProductCatalog products, Ports.PaymentMethodCatalog methods, Ports.CartRepository carts) {
        this.products = products; this.methods = methods; this.carts = carts;
    }

    public PreCheckoutResponse preCheckout(PreCheckoutRequest request) {
        if (request == null || request.products() == null || request.products().isEmpty()) throw new CheckoutError(Kind.INVALID_REQUEST, null);
        Set<String> slugs = new LinkedHashSet<>();
        for (var selection : request.products()) {
            if (selection == null || selection.slug() == null || selection.slug().isBlank() || !slugs.add(selection.slug())) throw new CheckoutError(Kind.INVALID_REQUEST, null);
        }
        List<Product> selected = new ArrayList<>();
        for (String slug : slugs) {
            try { selected.add(products.getProduct(slug)); }
            catch (CheckoutError ex) { throw ex; }
            catch (RuntimeException ex) { log.warn("Product dependency failed"); throw new CheckoutError(Kind.DEPENDENCY_UNAVAILABLE, null, ex); }
        }
        List<PaymentMethod> available;
        try { available = methods.getPaymentMethods(); }
        catch (RuntimeException ex) { log.warn("Payment method dependency failed"); throw new CheckoutError(Kind.DEPENDENCY_UNAVAILABLE, null, ex); }
        if (selected.isEmpty() || available == null || available.isEmpty()) throw new CheckoutError(Kind.DEPENDENCY_UNAVAILABLE, null);
        String cartId = UUID.randomUUID().toString();
        Cart cart = new Cart(cartId, "PENDENTE", List.copyOf(selected), List.copyOf(available));
        try { carts.create(cart); }
        catch (RuntimeException ex) { log.warn("Cart could not be stored; cartId={}", cartId); throw new CheckoutError(Kind.DEPENDENCY_UNAVAILABLE, null, ex); }
        log.info("Cart created; cartId={}", cartId);
        return new PreCheckoutResponse(cartId,
            selected.stream().map(p -> new ProductView(p.slug(), p.name(), p.description(), p.price(), p.currency())).toList(),
            available.stream().map(m -> new PaymentMethodView(m.name())).toList());
    }

    public CheckoutResponse checkout(CheckoutRequest request) {
        if (request == null || request.cartId() == null || request.cartId().isBlank() || request.paymentMethod() == null || request.paymentMethod().name() == null || request.paymentMethod().name().isBlank()) throw new CheckoutError(Kind.INVALID_REQUEST, request == null ? null : request.cartId());
        String cartId = request.cartId();
        Cart cart;
        try { cart = carts.find(cartId).orElseThrow(() -> new CheckoutError(Kind.CART_NOT_FOUND, cartId)); }
        catch (CheckoutError ex) { throw ex; }
        catch (RuntimeException ex) { log.warn("Cart lookup failed; cartId={}", cartId); throw new CheckoutError(Kind.DEPENDENCY_UNAVAILABLE, cartId, ex); }
        if ("APROVADO".equals(cart.status())) throw new CheckoutError(Kind.PAYMENT_ALREADY_APPROVED, cartId);
        if (cart.paymentMethods().stream().noneMatch(m -> m.name().equals(request.paymentMethod().name()))) throw new CheckoutError(Kind.PAYMENT_METHOD_NOT_ALLOWED, cartId);
        boolean pix = "Pix".equals(request.paymentMethod().name());
        Card card = request.paymentMethod().card();
        if (pix) {
            if (card != null) throw new CheckoutError(Kind.INVALID_REQUEST, cartId);
        } else if (!validCard(card)) {
            throw new CheckoutError(Kind.INVALID_REQUEST, cartId);
        }
        BigDecimal amount = cart.products().stream().map(Product::price).reduce(BigDecimal.ZERO, BigDecimal::add);
        if (!pix && card.number().endsWith("0002")) throw new CheckoutError(Kind.PAYMENT_DECLINED, cartId);

        ApprovalResult result;
        try { result = carts.approve(cartId); }
        catch (RuntimeException ex) { log.error("Approval result unknown; cartId={}", cartId); throw new CheckoutError(Kind.UNKNOWN_APPROVAL, cartId, ex); }
        if (result == ApprovalResult.ALREADY_APPROVED) throw new CheckoutError(Kind.PAYMENT_ALREADY_APPROVED, cartId);
        if (result == ApprovalResult.NOT_FOUND) throw new CheckoutError(Kind.CART_NOT_FOUND, cartId);

        PaymentView payment = pix
            ? new PaymentView(request.paymentMethod().name(), null, "PIX-SIMULADO-" + cartId, amount, "BRL")
            : new PaymentView(request.paymentMethod().name(), card.number().substring(card.number().length() - 4), null, amount, "BRL");
        log.info("Payment approved; cartId={}", cartId);
        return new CheckoutResponse(cartId, "APROVADO", Instant.now(), payment,
            cart.products().stream().map(p -> new CheckoutProductView(p.slug(), p.name(), p.price())).toList());
    }

    private boolean validCard(Card c) {
        return c != null && nonblank(c.holderName()) && c.number() != null && c.number().matches("\\d{4,}")
            && c.cvv() != null && c.cvv().matches("\\d+") && c.expiry() != null && c.expiry().matches("(0[1-9]|1[0-2])/\\d{2}");
    }
    private boolean nonblank(String value) { return value != null && !value.isBlank(); }
}
