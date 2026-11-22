package br.com.globo.checkout.worker.application;

import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import br.com.globo.checkout.worker.domain.CartSnapshot;
import java.time.Duration;
import java.util.Optional;
import java.util.List;

/** Application boundaries; transport and Redis adapters are supplied in later iterations. */
public final class PaymentPorts {
    private PaymentPorts() {}

    public interface CommandPublisher {
        PublishResult publish(PaymentCommand command, Duration timeout);
    }
    public interface FailureQueuePublisher {
        boolean publish(FailureNotice failure, Duration timeout);
    }
    public enum PublishResult { CONFIRMED, KNOWN_FAILURE, UNKNOWN }

    public interface AttemptStore {
        ReservationResult create(PaymentCommand command);
        Optional<AttemptSnapshot> find(String attemptId);
        Optional<AttemptSnapshot> findActiveByCart(String cartId);
        /** Durable v2 approval marker; true remains authoritative even if its attempt record is missing. */
        boolean isApprovedByCart(String cartId);
        List<String> findPublicationDue(int limit);
        PublishReservation reservePublication(String attemptId);
        boolean markPublicationExhausted(String attemptId);
        boolean confirmPublication(String attemptId);
        Optional<ProcessingLease> acquire(String attemptId);
        boolean renew(ProcessingLease lease);
        boolean releaseForRetry(ProcessingLease lease);
        Optional<Integer> reserveExecution(ProcessingLease lease);
        CompletionResult complete(ProcessingLease lease, PaymentResult result);
        boolean markIntervention(String attemptId, String safeReason);
        boolean markExecutionExhausted(ProcessingLease lease);
        boolean markOwnedIntervention(ProcessingLease lease, String safeReason);
        boolean authorizePublicationCycle(String attemptId);
        boolean authorizeProcessingCycle(String attemptId);
        LeaseInfo inspectLease(String attemptId);
    }
    public interface CartSnapshotReader { Optional<CartSnapshot> find(String cartId); }
    public enum ReservationResult { CREATED, ACTIVE_ATTEMPT, ALREADY_APPROVED, CART_NOT_FOUND, UNKNOWN }
    public enum PublishReservation { RESERVED, NOT_DUE, EXHAUSTED, INELIGIBLE, UNKNOWN }
    public enum CompletionResult { COMPLETED, LOST_OWNERSHIP, ALREADY_TERMINAL, UNKNOWN }
    public record FailureNotice(String attemptId, String cartId, String sourceTopic,
                                int sourcePartition, long sourceOffset, String reasonCode) {
        public FailureNotice {
            if (sourceTopic == null || sourceTopic.isBlank()) throw new IllegalArgumentException("source topic is required");
            if (reasonCode == null || !reasonCode.matches("[A-Z0-9_]{1,64}")) throw new IllegalArgumentException("safe reason code is required");
            if ((attemptId == null) != (cartId == null)) throw new IllegalArgumentException("attempt and cart identifiers must be present together");
        }
    }
    public record AttemptSnapshot(String attemptId, String cartId, PaymentCommand command, String state,
                                  int publicationCount, int executionCount,
                                  long nextPublicationAtMillis, long nextExecutionAtMillis,
                                  String safeReason, PaymentResult result) {
        /** Compatibility accessor for older internal readers; publication is the active action only before processing. */
        public long nextActionAtMillis() {
            return "PROCESSANDO".equals(state) ? nextExecutionAtMillis : nextPublicationAtMillis;
        }
    }
    public record ProcessingLease(String attemptId, String ownerToken, long expiresAtMillis) {}
    public record LeaseInfo(boolean owned, long expiresAtMillis) {}
}
