package br.com.globo.checkout.bff.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import br.com.globo.checkout.bff.api.Requests.CheckoutRequest;
import br.com.globo.checkout.bff.api.Requests.PaymentSelection;
import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.Ports;
import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
import java.time.Duration;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CheckoutV2ServiceTest {
    @Test void blockedRedisReadConsumesThePassedGlobalDeadlineAndDoesNotStartAttemptMutation() throws Exception {
        Ports.CartRepository carts = mock(Ports.CartRepository.class);
        PaymentPorts.AttemptStore attempts = mock(PaymentPorts.AttemptStore.class);
        PublicationCoordinator publications = mock(PublicationCoordinator.class);
        var executor = Executors.newFixedThreadPool(1);
        CountDownLatch lookupEntered = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        String cartId = UUID.randomUUID().toString();
        when(carts.find(cartId)).thenAnswer(invocation -> {
            lookupEntered.countDown();
            releaseLookup.await(2, TimeUnit.SECONDS);
            return Optional.empty();
        });
        CheckoutV2Service service = new CheckoutV2Service(carts, attempts, publications, executor);
        long deadline = System.nanoTime() + Duration.ofMillis(150).toNanos();
        long started = System.nanoTime();
        try {
            CheckoutError error = assertThrows(CheckoutError.class,
                () -> service.checkout(new CheckoutRequest(cartId, new PaymentSelection("Pix", null)), deadline));
            assertEquals(CheckoutError.Kind.DEPENDENCY_UNAVAILABLE, error.kind());
            assertTrue(lookupEntered.await(1, TimeUnit.SECONDS));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 900,
                "service must not restart the five-second budget after the request entered");
            verify(attempts, never()).create(any());
            verify(publications, never()).publish(anyString(), any());
        } finally {
            releaseLookup.countDown();
            executor.shutdownNow();
        }
    }

    @Test void expiredDeadlineDoesNotBeginCartReadOrAttemptMutation() {
        Ports.CartRepository carts = mock(Ports.CartRepository.class);
        PaymentPorts.AttemptStore attempts = mock(PaymentPorts.AttemptStore.class);
        PublicationCoordinator publications = mock(PublicationCoordinator.class);
        var executor = Executors.newFixedThreadPool(1);
        CheckoutV2Service service = new CheckoutV2Service(carts, attempts, publications, executor);
        String cartId = UUID.randomUUID().toString();
        try {
            assertThrows(CheckoutError.class, () -> service.checkout(
                new CheckoutRequest(cartId, new PaymentSelection("Pix", null)), System.nanoTime() - 1));
            verify(carts, never()).find(anyString());
            verify(attempts, never()).create(any());
            verify(publications, never()).publish(anyString(), any());
        } finally { executor.shutdownNow(); }
    }

    @Test void uncertainRedisCreateUsesExistingUnknownApprovalContract() {
        Ports.CartRepository carts = mock(Ports.CartRepository.class);
        PaymentPorts.AttemptStore attempts = mock(PaymentPorts.AttemptStore.class);
        PublicationCoordinator publications = mock(PublicationCoordinator.class);
        var executor = Executors.newFixedThreadPool(1);
        String cartId = UUID.randomUUID().toString();
        when(carts.find(cartId)).thenReturn(Optional.of(new Models.Cart(cartId, "PENDENTE",
            List.of(new Models.Product("basic", "Plano", "Assinatura", new BigDecimal("10.10"), "BRL")),
            List.of(new Models.PaymentMethod("Pix")))));
        when(attempts.create(any())).thenThrow(new IllegalStateException("simulated lost Redis response"));
        CheckoutV2Service service = new CheckoutV2Service(carts, attempts, publications, executor);
        try {
            CheckoutError error = assertThrows(CheckoutError.class, () -> service.checkout(
                new CheckoutRequest(cartId, new PaymentSelection("Pix", null)), System.nanoTime() + Duration.ofSeconds(2).toNanos()));
            assertEquals(CheckoutError.Kind.UNKNOWN_APPROVAL, error.kind());
            verify(publications, never()).publish(anyString(), any());
        } finally { executor.shutdownNow(); }
    }
}
