package br.com.globo.checkout.bff.application;

import br.com.globo.checkout.bff.api.Requests.Card;
import br.com.globo.checkout.bff.api.Requests.CheckoutRequest;
import br.com.globo.checkout.bff.api.Responses;
import br.com.globo.checkout.bff.api.PaymentInProgressException;
import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.Models.Cart;
import br.com.globo.checkout.bff.domain.Models.Product;
import br.com.globo.checkout.bff.domain.Ports;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.springframework.stereotype.Service;

/** HTTP-facing orchestration for v2; it returns v1 response and business-error contracts. */
@Service
public class CheckoutV2Service {
    private static final long POLL_NANOS = Duration.ofMillis(100).toNanos();
    private final Ports.CartRepository carts;
    private final PaymentPorts.AttemptStore attempts;
    private final PublicationCoordinator publications;
    private final ExecutorService dependencyExecutor;

    public CheckoutV2Service(Ports.CartRepository carts, PaymentPorts.AttemptStore attempts,
                             PublicationCoordinator publications, ExecutorService v2OperationExecutor) {
        this.carts = carts;
        this.attempts = attempts;
        this.publications = publications;
        this.dependencyExecutor = v2OperationExecutor;
    }

    public Responses.CheckoutResponse checkout(CheckoutRequest request, long deadline) {
        if (request == null || request.cartId() == null || request.cartId().isBlank()
            || request.paymentMethod() == null || request.paymentMethod().name() == null
            || request.paymentMethod().name().isBlank()) {
            throw new CheckoutError(CheckoutError.Kind.INVALID_REQUEST, request == null ? null : request.cartId());
        }
        String cartId = request.cartId();
        Cart cart;
        try {
            cart = call(deadline, () -> carts.find(cartId).orElse(null));
        } catch (RuntimeException ex) {
            throw dependencyFailure(cartId);
        }
        if (cart == null) throw new CheckoutError(CheckoutError.Kind.CART_NOT_FOUND, cartId);
        if (!cartId.equals(cart.cartId())) throw new CheckoutError(CheckoutError.Kind.CART_NOT_FOUND, cartId);

        // Match v1 precedence: an already approved cart wins before method or card validation.
        if ("APROVADO".equals(cart.status())) throw new CheckoutError(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, cartId);
        try {
            if (call(deadline, () -> attempts.isApprovedByCart(cartId)))
                throw new CheckoutError(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, cartId);
            var active = call(deadline, () -> attempts.findActiveByCart(cartId).orElse(null));
            if (active != null) {
                if ("APROVADO".equals(active.state()) && active.result() != null)
                    throw new CheckoutError(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, cartId);
                if ("RECUSADO".equals(active.state()) && active.result() != null) {
                    // A refused v2 attempt permits another attempt, matching the synchronous v1 rule.
                } else if ("APROVADO".equals(active.state()) || "RECUSADO".equals(active.state())) {
                    throw unknownApproval(cartId);
                } else {
                    throw new PaymentInProgressException(cartId);
                }
            }
        } catch (CheckoutError | PaymentInProgressException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw dependencyFailure(cartId);
        }
        String method = request.paymentMethod().name();
        if (cart.paymentMethods() == null || cart.paymentMethods().stream().noneMatch(m -> m != null && method.equals(m.name()))) {
            throw new CheckoutError(CheckoutError.Kind.PAYMENT_METHOD_NOT_ALLOWED, cartId);
        }
        boolean pix = PaymentCommand.PIX.equals(method);
        Card card = request.paymentMethod().card();
        if (pix ? card != null : !validCard(card)) throw new CheckoutError(CheckoutError.Kind.INVALID_REQUEST, cartId);

        // Resolve a non-UUID unknown id as CART_NOT_FOUND before constructing the UUID-only Kafka command.
        String attemptId = java.util.UUID.randomUUID().toString();
        PaymentCommand command = new PaymentCommand(PaymentCommand.CURRENT_SCHEMA_VERSION, attemptId, cart.cartId(),
            method, pix ? null : card.number().substring(card.number().length() - 4));
        final PaymentPorts.ReservationResult reservation;
        try {
            reservation = call(deadline, () -> attempts.create(command));
        } catch (RuntimeException ex) {
            // CREATE is a Redis mutation. Once submitted, timeout/connection loss cannot establish
            // whether the reservation happened, so preserve the original unknown-result contract.
            throw unknownApproval(cartId);
        }
        switch (reservation) {
            case CREATED -> { }
            case ACTIVE_ATTEMPT -> throw new PaymentInProgressException(cartId);
            case ALREADY_APPROVED -> throw new CheckoutError(CheckoutError.Kind.PAYMENT_ALREADY_APPROVED, cartId);
            case CART_NOT_FOUND -> throw new CheckoutError(CheckoutError.Kind.CART_NOT_FOUND, cartId);
            case UNKNOWN -> throw unknownApproval(cartId);
        }
        if (remaining(deadline) <= 0) throw unknownApproval(cartId);

        final PublicationCoordinator.Result publication;
        try {
            publication = call(deadline, () -> publications.publish(attemptId, Duration.ofNanos(remaining(deadline))));
        } catch (RuntimeException ex) {
            throw unknownApproval(cartId);
        }
        if (publication == PublicationCoordinator.Result.KNOWN_FAILURE) throw dependencyFailure(cartId);
        if (publication != PublicationCoordinator.Result.CONFIRMED && publication != PublicationCoordinator.Result.UNKNOWN)
            throw unknownApproval(cartId);

        while (remaining(deadline) > 0) {
            final PaymentPorts.AttemptSnapshot snapshot;
            try {
                snapshot = call(deadline, () -> attempts.find(attemptId).orElse(null));
            } catch (RuntimeException ex) {
                throw unknownApproval(cartId);
            }
            if (snapshot == null) throw unknownApproval(cartId);
            if (snapshot.result() != null && ("APROVADO".equals(snapshot.state()) || "RECUSADO".equals(snapshot.state()))) {
                return responseOrDecline(snapshot.result());
            }
            long remaining = remaining(deadline);
            if (remaining <= 0) break;
            try { TimeUnit.NANOSECONDS.sleep(Math.min(POLL_NANOS, remaining)); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw dependencyFailure(cartId); }
        }
        throw unknownApproval(cartId);
    }

    private <T> T call(long deadline, Supplier<T> operation) {
        long remaining = remaining(deadline);
        if (remaining <= 0) throw new OperationDeadlineExceeded();
        final Future<T> future;
        try {
            future = dependencyExecutor.submit(() -> {
                if (remaining(deadline) <= 0) throw new OperationDeadlineExceeded();
                return operation.get();
            });
        }
        catch (RejectedExecutionException ex) { throw new OperationDeadlineExceeded(); }
        try { return future.get(remaining, TimeUnit.NANOSECONDS); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new OperationDeadlineExceeded(); }
        catch (TimeoutException ex) { throw new OperationDeadlineExceeded(); }
        catch (ExecutionException ex) { throw ex.getCause() instanceof RuntimeException runtime ? runtime : new IllegalStateException(ex.getCause()); }
    }

    private Responses.CheckoutResponse responseOrDecline(PaymentResult result) {
        if (result.outcome() == PaymentResult.Outcome.DECLINED) {
            throw new CheckoutError(CheckoutError.Kind.PAYMENT_DECLINED, result.cartId());
        }
        var payment = new Responses.PaymentView(result.paymentMethod(), result.lastDigits(), result.pixCode(), result.amount(), result.currency());
        var products = result.products().stream().map(p -> new Responses.CheckoutProductView(p.slug(), p.name(), p.price())).toList();
        return new Responses.CheckoutResponse(result.cartId(), "APROVADO", result.paidAt(), payment, products);
    }

    private static boolean validCard(Card card) {
        return card != null && nonblank(card.holderName()) && card.number() != null && card.number().matches("\\d{4,}")
            && card.cvv() != null && card.cvv().matches("\\d+") && card.expiry() != null
            && card.expiry().matches("(0[1-9]|1[0-2])/\\d{2}");
    }
    private static boolean nonblank(String value) { return value != null && !value.isBlank(); }
    private static CheckoutError dependencyFailure(String cartId) { return new CheckoutError(CheckoutError.Kind.DEPENDENCY_UNAVAILABLE, cartId); }
    private static CheckoutError unknownApproval(String cartId) { return new CheckoutError(CheckoutError.Kind.UNKNOWN_APPROVAL, cartId); }
    private static long remaining(long deadline) { return Math.max(0, deadline - System.nanoTime()); }
    private static long saturatedAdd(long left, long right) { long result = left + right; return result < 0 && left > 0 ? Long.MAX_VALUE : result; }

    private static final class OperationDeadlineExceeded extends RuntimeException {}
}
