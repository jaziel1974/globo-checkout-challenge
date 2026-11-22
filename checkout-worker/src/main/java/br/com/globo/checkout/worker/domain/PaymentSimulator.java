package br.com.globo.checkout.worker.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Transport-independent payment rules for a v2 attempt, based only on the cart snapshot. */
public final class PaymentSimulator {
    public PaymentResult simulate(PaymentCommand command, CartSnapshot cart) {
        if (command == null || cart == null || !command.cartId().equals(cart.cartId())) {
            throw new PaymentBusinessException(PaymentBusinessException.Kind.CART_NOT_FOUND);
        }
        if ("APROVADO".equals(cart.status())) {
            throw new PaymentBusinessException(PaymentBusinessException.Kind.PAYMENT_ALREADY_APPROVED);
        }
        if (!cart.paymentMethods().contains(command.paymentMethod())) {
            throw new PaymentBusinessException(PaymentBusinessException.Kind.PAYMENT_METHOD_NOT_ALLOWED);
        }
        if (cart.products() == null || cart.products().isEmpty()
            || cart.products().stream().anyMatch(product -> product == null || product.price() == null || product.slug() == null || product.name() == null || !"BRL".equals(product.currency()))) {
            throw new PaymentBusinessException(PaymentBusinessException.Kind.INVALID_REQUEST);
        }

        BigDecimal amount = cart.products().stream().map(CartSnapshot.Product::price).reduce(BigDecimal.ZERO, BigDecimal::add);
        List<PaymentResult.ProductResult> products = cart.products().stream()
            .map(product -> new PaymentResult.ProductResult(product.slug(), product.name(), product.price())).toList();
        boolean pix = PaymentCommand.PIX.equals(command.paymentMethod());
        if (!pix && "0002".equals(command.lastDigits())) {
            return new PaymentResult(cart.cartId(), PaymentResult.Outcome.DECLINED, null,
                command.paymentMethod(), command.lastDigits(), null, amount, "BRL", products, "PAYMENT_DECLINED");
        }
        return new PaymentResult(cart.cartId(), PaymentResult.Outcome.APPROVED, Instant.now(),
            command.paymentMethod(), pix ? null : command.lastDigits(), pix ? "PIX-SIMULADO-" + cart.cartId() : null,
            amount, "BRL", products, null);
    }
}
