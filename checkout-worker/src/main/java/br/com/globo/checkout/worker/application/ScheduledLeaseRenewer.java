package br.com.globo.checkout.worker.application;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Renews a Redis processing lease every configured interval and fails closed on any lost renewal. */
public final class ScheduledLeaseRenewer {
    private final PaymentPorts.AttemptStore attempts;
    private final ScheduledExecutorService scheduler;
    private final long intervalMillis;

    public ScheduledLeaseRenewer(PaymentPorts.AttemptStore attempts,
                                 ScheduledExecutorService scheduler, long intervalMillis) {
        if (intervalMillis <= 0) throw new IllegalArgumentException("renewal interval must be positive");
        this.attempts = attempts;
        this.scheduler = scheduler;
        this.intervalMillis = intervalMillis;
    }

    public Guard start(PaymentPorts.ProcessingLease lease) {
        AtomicBoolean lost = new AtomicBoolean();
        ScheduledFuture<?> renewal = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!attempts.renew(lease)) lost.set(true);
            } catch (RuntimeException e) {
                lost.set(true);
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        return new Guard(lost, renewal);
    }

    public static final class Guard implements AutoCloseable {
        private final AtomicBoolean lost;
        private final ScheduledFuture<?> renewal;
        private Guard(AtomicBoolean lost, ScheduledFuture<?> renewal) { this.lost = lost; this.renewal = renewal; }
        public boolean isOwned() { return !lost.get(); }
        @Override public void close() { renewal.cancel(false); }
    }
}
