package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

class KafkaCommandPublisherTest {
    @Test void timesOutWhileSendCallIsBlockedWithoutCancellingTheSendThread() throws Exception {
        var kafka = mock(KafkaTemplate.class);
        var enteredSend = new CountDownLatch(1);
        var releaseSend = new CountDownLatch(1);
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(call -> {
            enteredSend.countDown();
            releaseSend.await();
            return new CompletableFuture<SendResult<String, String>>();
        });
        var executor = executor();
        try {
            KafkaCommandPublisher publisher = new KafkaCommandPublisher(kafka, new ObjectMapper(), executor, "commands");
            long started = System.nanoTime();
            var result = publisher.publish(command(), Duration.ofMillis(150));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertTrue(enteredSend.await(1, TimeUnit.SECONDS));
            assertEquals(PaymentPorts.PublishResult.UNKNOWN, result);
            assertTrue(elapsedMillis < 800, "blocked Kafka send consumed the HTTP thread budget");
        } finally {
            releaseSend.countDown();
            executor.shutdownNow();
        }
    }

    @Test void acknowledgementTimeoutIsUnknownAndDoesNotCancelTheBrokerFuture() throws Exception {
        var kafka = mock(KafkaTemplate.class);
        var enteredSend = new CountDownLatch(1);
        var pendingAck = new CompletableFuture<SendResult<String, String>>();
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(call -> {
            enteredSend.countDown();
            return pendingAck;
        });
        var executor = executor();
        try {
            KafkaCommandPublisher publisher = new KafkaCommandPublisher(kafka, new ObjectMapper(), executor, "commands");
            var result = publisher.publish(command(), Duration.ofMillis(150));
            assertTrue(enteredSend.await(1, TimeUnit.SECONDS));
            assertEquals(PaymentPorts.PublishResult.UNKNOWN, result);
            assertFalse(pendingAck.isCancelled());
        } finally {
            executor.shutdownNow();
        }
    }

    private static PaymentCommand command() {
        return new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            PaymentCommand.CREDIT, "4242");
    }
    private static ThreadPoolExecutor executor() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            task -> { Thread thread = new Thread(task, "publisher-test"); thread.setDaemon(true); return thread; },
            new ThreadPoolExecutor.AbortPolicy());
    }
}
