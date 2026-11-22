package br.com.globo.checkout.worker.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import br.com.globo.checkout.worker.application.PaymentPorts.AttemptSnapshot;
import br.com.globo.checkout.worker.application.PaymentPorts.AttemptStore;
import br.com.globo.checkout.worker.application.PaymentPorts.CommandPublisher;
import br.com.globo.checkout.worker.application.PaymentPorts.PublishReservation;
import br.com.globo.checkout.worker.application.PaymentPorts.PublishResult;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PublicationCoordinatorTest {
    @Test void reservesBeforeSendingAndMarksInterventionWhenThirdSendIsUnconfirmed() {
        String cartId = UUID.randomUUID().toString();
        PaymentCommand command = new PaymentCommand(1, UUID.randomUUID().toString(), cartId,
            PaymentCommand.CREDIT, "4242");
        AttemptSnapshot before = snapshot(command, 2);
        AttemptSnapshot reserved = snapshot(command, 3);
        AttemptStore store = mock(AttemptStore.class);
        CommandPublisher publisher = mock(CommandPublisher.class);
        when(store.find(command.attemptId())).thenReturn(Optional.of(before), Optional.of(reserved));
        when(store.reservePublication(command.attemptId())).thenReturn(PublishReservation.RESERVED);
        when(publisher.publish(eq(command), any())).thenReturn(PublishResult.UNKNOWN);

        PublicationCoordinator coordinator = new PublicationCoordinator(store, publisher);
        assertEquals(PublicationCoordinator.Result.UNKNOWN, coordinator.publish(command.attemptId(), Duration.ofSeconds(2)));

        var order = inOrder(store, publisher);
        order.verify(store).reservePublication(command.attemptId());
        order.verify(publisher).publish(eq(command), any());
        order.verify(store).markPublicationExhausted(command.attemptId());
    }

    @Test void thirdReservationThatRunsOutOfBudgetStillMarksInterventionWithoutSending() {
        String cartId = UUID.randomUUID().toString();
        PaymentCommand command = new PaymentCommand(1, UUID.randomUUID().toString(), cartId,
            PaymentCommand.CREDIT, "4242");
        AttemptStore store = mock(AttemptStore.class);
        CommandPublisher publisher = mock(CommandPublisher.class);
        when(store.find(command.attemptId())).thenReturn(Optional.of(snapshot(command, 2)), Optional.of(snapshot(command, 3)));
        when(store.reservePublication(command.attemptId())).thenAnswer(call -> {
            Thread.sleep(250);
            return PublishReservation.RESERVED;
        });

        PublicationCoordinator coordinator = new PublicationCoordinator(store, publisher);
        assertEquals(PublicationCoordinator.Result.UNKNOWN, coordinator.publish(command.attemptId(), Duration.ofMillis(200)));
        verify(publisher, never()).publish(any(), any());
        verify(store, never()).markPublicationExhausted(command.attemptId());
        verify(store, times(1)).find(command.attemptId());
    }

    @Test void slowLookupThatFinishesAfterDeadlineDoesNotStartReservationOrSend() {
        String cartId = UUID.randomUUID().toString();
        PaymentCommand command = new PaymentCommand(1, UUID.randomUUID().toString(), cartId,
            PaymentCommand.CREDIT, "4242");
        AttemptStore store = mock(AttemptStore.class);
        CommandPublisher publisher = mock(CommandPublisher.class);
        when(store.find(command.attemptId())).thenAnswer(call -> {
            Thread.sleep(250);
            return Optional.of(snapshot(command, 0));
        });

        PublicationCoordinator coordinator = new PublicationCoordinator(store, publisher);
        assertEquals(PublicationCoordinator.Result.UNKNOWN, coordinator.publish(command.attemptId(), Duration.ofMillis(200)));
        verify(store, never()).reservePublication(command.attemptId());
        verify(publisher, never()).publish(any(), any());
    }

    private static AttemptSnapshot snapshot(PaymentCommand command, int publicationCount) {
        return new AttemptSnapshot(command.attemptId(), command.cartId(), command, "AGUARDANDO_PUBLICACAO",
            publicationCount, 0, 0, 0, null, null);
    }
}
