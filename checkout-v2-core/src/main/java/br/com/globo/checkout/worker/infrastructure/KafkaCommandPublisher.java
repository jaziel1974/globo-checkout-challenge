package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

/** Publishes the minimal v2 command under one monotonic deadline without blocking HTTP threads. */
public final class KafkaCommandPublisher implements PaymentPorts.CommandPublisher {
    private static final Duration MAX_SEND_BUDGET = Duration.ofSeconds(2);
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final ExecutorService sendExecutor;
    private final String topic;

    public KafkaCommandPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper,
                                 ExecutorService sendExecutor, String topic) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.sendExecutor = sendExecutor;
        this.topic = topic;
    }

    @Override public PaymentPorts.PublishResult publish(PaymentCommand command, Duration timeout) {
        long started = System.nanoTime();
        long budget = Math.min(positiveNanos(timeout), MAX_SEND_BUDGET.toNanos());
        long deadline = saturatedAdd(started, budget);

        final String payload;
        try {
            payload = mapper.writeValueAsString(command.toWireFields());
        } catch (Exception e) {
            return PaymentPorts.PublishResult.KNOWN_FAILURE;
        }
        if (remaining(deadline) <= 0) return PaymentPorts.PublishResult.UNKNOWN;

        final Future<PaymentPorts.PublishResult> send;
        try {
            send = sendExecutor.submit(() -> {
                try {
                    if (remaining(deadline) <= 0) return PaymentPorts.PublishResult.KNOWN_FAILURE;
                    var brokerResult = kafka.send(topic, command.cartId(), payload);
                    long remaining = remaining(deadline);
                    if (remaining <= 0) return PaymentPorts.PublishResult.UNKNOWN;
                    brokerResult.get(remaining, TimeUnit.NANOSECONDS);
                    return PaymentPorts.PublishResult.CONFIRMED;
                } catch (Exception e) {
                    // Once send has been invoked, an exception does not prove that Kafka did not append it.
                    return PaymentPorts.PublishResult.UNKNOWN;
                }
            });
        } catch (RejectedExecutionException e) {
            // No send started: the bounded, non-queuing publisher had no capacity.
            return PaymentPorts.PublishResult.KNOWN_FAILURE;
        }

        try {
            long remaining = remaining(deadline);
            if (remaining <= 0) return PaymentPorts.PublishResult.UNKNOWN;
            return send.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            // Deliberately do not cancel: timeout does not cancel a send already accepted by the client.
            return PaymentPorts.PublishResult.UNKNOWN;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return PaymentPorts.PublishResult.UNKNOWN;
        } catch (ExecutionException e) {
            return PaymentPorts.PublishResult.UNKNOWN;
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
}
