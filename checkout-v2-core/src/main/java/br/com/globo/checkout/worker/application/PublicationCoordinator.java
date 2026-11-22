package br.com.globo.checkout.worker.application;

import br.com.globo.checkout.worker.application.PaymentPorts.AttemptSnapshot;
import br.com.globo.checkout.worker.application.PaymentPorts.AttemptStore;
import br.com.globo.checkout.worker.application.PaymentPorts.CommandPublisher;
import br.com.globo.checkout.worker.application.PaymentPorts.PublishReservation;
import br.com.globo.checkout.worker.application.PaymentPorts.PublishResult;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reserves each application-level send before calling Kafka and records only confirmed publication. */
public final class PublicationCoordinator {
    private static final Logger log = LoggerFactory.getLogger(PublicationCoordinator.class);
    private static final long MAX_BUDGET_NANOS = Duration.ofSeconds(2).toNanos();
    private final AttemptStore attempts;
    private final CommandPublisher publisher;

    public PublicationCoordinator(AttemptStore attempts, CommandPublisher publisher) {
        this.attempts = attempts;
        this.publisher = publisher;
    }

    public Result publish(String attemptId, Duration callerBudget) {
        long started = System.nanoTime();
        long budget = positiveNanos(callerBudget);
        long deadline = saturatedAdd(started, Math.min(budget, MAX_BUDGET_NANOS));
        if (remaining(deadline) <= 0) return Result.UNKNOWN;

        Optional<AttemptSnapshot> found = attempts.find(attemptId);
        if (found.isEmpty()) return Result.NOT_FOUND;
        if (remaining(deadline) <= 0) return Result.UNKNOWN;
        PublishReservation reservation = attempts.reservePublication(attemptId);
        if (reservation != PublishReservation.RESERVED) return switch (reservation) {
            case NOT_DUE -> Result.NOT_DUE;
            case EXHAUSTED -> Result.EXHAUSTED;
            case INELIGIBLE -> Result.INELIGIBLE;
            case UNKNOWN -> Result.UNKNOWN;
            case RESERVED -> throw new IllegalStateException("unreachable");
        };

        if (remaining(deadline) <= 0) return Result.UNKNOWN;
        Optional<AttemptSnapshot> reservedSnapshot = attempts.find(attemptId);
        if (reservedSnapshot.isEmpty()) return Result.UNKNOWN;

        long remaining = remaining(deadline);
        if (remaining <= 0) {
            exhaustIfThird(reservedSnapshot.get());
            return Result.UNKNOWN;
        }
        PublishResult published = publisher.publish(reservedSnapshot.get().command(), Duration.ofNanos(remaining));
        if (published == PublishResult.CONFIRMED) return attempts.confirmPublication(attemptId) ? Result.CONFIRMED : Result.UNKNOWN;
        exhaustIfThird(reservedSnapshot.get());
        return published == PublishResult.KNOWN_FAILURE ? Result.KNOWN_FAILURE : Result.UNKNOWN;
    }

    private void exhaustIfThird(AttemptSnapshot reservedSnapshot) {
        if (reservedSnapshot.publicationCount() >= 3
            && attempts.markPublicationExhausted(reservedSnapshot.attemptId())) {
            log.error("Checkout v2 publication exhausted attemptId={} cartId={} reasonCode=PUBLICATION_EXHAUSTED",
                reservedSnapshot.attemptId(), reservedSnapshot.cartId());
        }
    }

    private static long positiveNanos(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) return 0;
        try { return duration.toNanos(); }
        catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }
    private static long saturatedAdd(long left, long right) {
        long result = left + right;
        return result < 0 && left > 0 ? Long.MAX_VALUE : result;
    }
    private static long remaining(long deadline) { return Math.max(0, deadline - System.nanoTime()); }

    public enum Result { CONFIRMED, KNOWN_FAILURE, UNKNOWN, NOT_DUE, EXHAUSTED, INELIGIBLE, NOT_FOUND }
}
