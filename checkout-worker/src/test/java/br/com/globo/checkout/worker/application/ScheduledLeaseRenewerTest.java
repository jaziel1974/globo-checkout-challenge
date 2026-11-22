package br.com.globo.checkout.worker.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ScheduledLeaseRenewerTest {
    @Test void marksLeaseLostWhenPeriodicRedisRenewalFails() throws Exception {
        var attempts = mock(PaymentPorts.AttemptStore.class);
        var renewed = new CountDownLatch(1);
        when(attempts.renew(any())).thenAnswer(call -> { renewed.countDown(); return false; });
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var renewer = new ScheduledLeaseRenewer(attempts, scheduler, 10);
            var lease = new PaymentPorts.ProcessingLease(UUID.randomUUID().toString(), UUID.randomUUID().toString(), 0);
            try (var guard = renewer.start(lease)) {
                assertTrue(renewed.await(1, TimeUnit.SECONDS));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (guard.isOwned() && System.nanoTime() < until) Thread.yield();
                assertFalse(guard.isOwned());
            }
        } finally { scheduler.shutdownNow(); }
    }
}
