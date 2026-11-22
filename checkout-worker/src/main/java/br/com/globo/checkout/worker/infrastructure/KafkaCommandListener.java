package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.worker.application.PaymentCommandHandler;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import br.com.globo.checkout.worker.application.PaymentPorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sequential manual-ack listener. Unresolved records are nacked so later offsets cannot overtake them. */
@Component
@org.springframework.context.annotation.Profile("!worker-cli")
public final class KafkaCommandListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaCommandListener.class);
    private final PaymentCommandHandler handler;
    private final PaymentPorts.FailureQueuePublisher failures;
    private final ConcurrentHashMap<String, Integer> redisRetries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PaymentPorts.FailureNotice> pendingFailures = new ConcurrentHashMap<>();
    private final java.util.Set<String> redisWaitLogged = ConcurrentHashMap.newKeySet();

    public KafkaCommandListener(PaymentCommandHandler handler, PaymentPorts.FailureQueuePublisher failures) {
        this.handler = handler;
        this.failures = failures;
    }

    @KafkaListener(topics = "${checkout.kafka.command-topic:checkout-v2-commands}",
        groupId = "${spring.kafka.consumer.group-id:checkout-v2-worker}",
        containerFactory = "checkoutKafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String id = record.topic() + ":" + record.partition() + ":" + record.offset();
        PaymentPorts.FailureNotice pending = pendingFailures.get(id);
        if (pending != null) {
            forwardPending(id, pending, acknowledgment);
            return;
        }
        if (redisRetries.getOrDefault(id, 0) >= 3) {
            resolveRedisOutage(id, record, acknowledgment);
            return;
        }
        var result = handler.handleDetailed(record.key(), record.value());
        if (result.outcome() == PaymentCommandHandler.Outcome.ACKNOWLEDGE) {
            redisRetries.remove(id);
            acknowledgment.acknowledge();
            return;
        }
        if (result.outcome() == PaymentCommandHandler.Outcome.RETRY) {
            acknowledgment.nack(Duration.ofSeconds(1));
            return;
        }
        String reason = result.reasonCode() == null ? "PROCESSING_FAILED" : result.reasonCode();
        if (result.outcome() == PaymentCommandHandler.Outcome.REDIS_UNAVAILABLE) {
            int tries = redisRetries.merge(id, 1, Integer::sum);
            if (tries < 3) {
                acknowledgment.nack(Duration.ofSeconds(1));
                return;
            }
            if (redisWaitLogged.add(id)) log.warn(
                "Checkout command waiting for Redis before failure forwarding attemptId={} cartId={} sourceTopic={} sourcePartition={} sourceOffset={} reason=REDIS_UNAVAILABLE",
                result.attemptId(), result.cartId(), record.topic(), record.partition(), record.offset());
            resolveRedisOutage(id, record, acknowledgment);
            return;
        }
        var notice = new PaymentPorts.FailureNotice(result.attemptId(), result.cartId(), record.topic(),
            record.partition(), record.offset(), reason);
        pendingFailures.put(id, notice);
        forwardPending(id, notice, acknowledgment);
    }

    private void resolveRedisOutage(String id, org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record,
                                    Acknowledgment acknowledgment) {
        final PaymentCommandHandler.RedisFailureAttempt resolved;
        try { resolved = handler.resolveRedisFailure(record.key(), record.value()); }
        catch (RuntimeException redisUnavailable) {
            acknowledgment.nack(Duration.ofSeconds(1));
            return;
        }
        if (resolved.resolution() == PaymentCommandHandler.RedisFailureResolution.TERMINAL) {
            redisRetries.remove(id);
            redisWaitLogged.remove(id);
            acknowledgment.acknowledge();
            return;
        }
        if (resolved.resolution() == PaymentCommandHandler.RedisFailureResolution.UNRESOLVED) {
            acknowledgment.nack(Duration.ofSeconds(1));
            return;
        }
        var notice = new PaymentPorts.FailureNotice(resolved.attemptId(), resolved.cartId(), record.topic(),
            record.partition(), record.offset(), resolved.reasonCode());
        pendingFailures.put(id, notice);
        forwardPending(id, notice, acknowledgment);
    }

    private void forwardPending(String id, PaymentPorts.FailureNotice notice, Acknowledgment acknowledgment) {
        if (failures.publish(notice, Duration.ofSeconds(2))) {
            redisRetries.remove(id);
            redisWaitLogged.remove(id);
            pendingFailures.remove(id);
            log.warn("Checkout command forwarded to failure topic attemptId={} sourceTopic={} sourcePartition={} sourceOffset={} reason={}",
                notice.attemptId(), notice.sourceTopic(), notice.sourcePartition(), notice.sourceOffset(), notice.reasonCode());
            acknowledgment.acknowledge();
        } else {
            acknowledgment.nack(Duration.ofSeconds(1));
        }
    }
}
