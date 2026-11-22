package br.com.globo.checkout.worker.application;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Finds publication recovery work independently of Kafka command delivery. */
@Component
@org.springframework.context.annotation.Profile("!worker-cli")
public final class PublicationRecoveryScheduler {
    private static final Logger log = LoggerFactory.getLogger(PublicationRecoveryScheduler.class);
    private final PaymentPorts.AttemptStore attempts;
    private final PublicationCoordinator publications;
    private final Executor recoveryExecutor;
    public PublicationRecoveryScheduler(PaymentPorts.AttemptStore attempts, PublicationCoordinator publications,
                                        Executor recoveryExecutor) {
        this.attempts = attempts;
        this.publications = publications;
        this.recoveryExecutor = recoveryExecutor;
    }

    @Scheduled(fixedDelayString = "${checkout.kafka.recovery-interval-ms:1000}")
    public void recoverDuePublications() {
        try {
            for (String attemptId : attempts.findPublicationDue(20)) {
                try {
                    recoveryExecutor.execute(() -> {
                        try {
                            publications.publish(attemptId, Duration.ofSeconds(2));
                        }
                        catch (RuntimeException e) { log.warn("Kafka publication recovery deferred reason=SAFE_RETRY"); }
                    });
                } catch (RejectedExecutionException e) {
                    // Backpressure is safe: no Redis reservation was made, so the next scan may retry.
                    break;
                }
            }
        } catch (RuntimeException e) {
            // Do not emit Redis/client exception text or any persisted command fields.
            log.warn("Kafka publication recovery deferred reason=SAFE_RETRY");
        }
    }
}
