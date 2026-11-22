package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.worker.application.PaymentPorts;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/** Publishes metadata-only failure records and requires broker confirmation. */
@Component
public final class KafkaFailureQueuePublisher implements PaymentPorts.FailureQueuePublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaFailureQueuePublisher.class);
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final String topic;
    private final ExecutorService executor;

    public KafkaFailureQueuePublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper,
            @org.springframework.beans.factory.annotation.Value("${checkout.kafka.failure-topic:checkout-v2-failures}") String topic,
            @org.springframework.beans.factory.annotation.Qualifier("failureSendExecutor") ExecutorService executor) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.topic = topic;
        this.executor = executor;
    }

    @Override public boolean publish(PaymentPorts.FailureNotice failure, Duration timeout) {
        long start = System.nanoTime();
        long budget = timeout == null ? 0 : Math.max(0, timeout.toNanos());
        long deadline = start + budget;
        AtomicBoolean invoked = new AtomicBoolean();
        try {
            var fields = new LinkedHashMap<String, Object>();
            fields.put("schemaVersion", 1);
            if (failure.attemptId() != null) {
                fields.put("attemptId", failure.attemptId());
                fields.put("cartId", failure.cartId());
            }
            fields.put("sourceTopic", failure.sourceTopic());
            fields.put("sourcePartition", failure.sourcePartition());
            fields.put("sourceOffset", failure.sourceOffset());
            fields.put("reasonCode", failure.reasonCode());
            String payload = mapper.writeValueAsString(fields);
            Future<Boolean> task = executor.submit(() -> {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                invoked.set(true);
                kafka.send(topic, failure.attemptId(), payload).get(remaining, TimeUnit.NANOSECONDS);
                return true;
            });
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException();
            return task.get(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Kafka failure forwarding failed topic={} attemptId={} sourceTopic={} sourcePartition={} sourceOffset={} reason={} cause=INTERRUPTED",
                topic, failure.attemptId(), failure.sourceTopic(), failure.sourcePartition(), failure.sourceOffset(), failure.reasonCode());
        } catch (TimeoutException e) {
            log.warn("Kafka failure forwarding failed topic={} attemptId={} sourceTopic={} sourcePartition={} sourceOffset={} reason={} cause=TIMEOUT",
                topic, failure.attemptId(), failure.sourceTopic(), failure.sourcePartition(), failure.sourceOffset(), failure.reasonCode());
        } catch (ExecutionException e) {
            log.warn("Kafka failure forwarding failed topic={} attemptId={} sourceTopic={} sourcePartition={} sourceOffset={} reason={} cause=PRODUCER_ERROR",
                topic, failure.attemptId(), failure.sourceTopic(), failure.sourcePartition(), failure.sourceOffset(), failure.reasonCode());
        } catch (RejectedExecutionException e) {
            log.warn("Kafka failure forwarding failed topic={} attemptId={} sourceTopic={} sourcePartition={} sourceOffset={} reason={} cause=CAPACITY",
                topic, failure.attemptId(), failure.sourceTopic(), failure.sourcePartition(), failure.sourceOffset(), failure.reasonCode());
        } catch (Exception ignored) {
            log.warn("Kafka failure forwarding failed topic={} attemptId={} sourceTopic={} sourcePartition={} sourceOffset={} reason={} cause=SERIALIZATION_ERROR",
                topic, failure.attemptId(), failure.sourceTopic(), failure.sourcePartition(), failure.sourceOffset(), failure.reasonCode());
        }
        if (invoked.get()) {
            // A timeout after send is uncertain, so the source offset remains unresolved for safe duplication.
        }
        return false;
    }
}
