package br.com.globo.checkout.worker.infrastructure;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

/** Logs only safe routing identifiers and a fixed reason; never prints Kafka record values or exceptions. */
public final class SafeKafkaProducerListener implements ProducerListener<String, String> {
    private static final Logger log = LoggerFactory.getLogger(SafeKafkaProducerListener.class);

    @Override public void onError(ProducerRecord<String, String> record, RecordMetadata metadata, Exception exception) {
        log.warn("Kafka publication failed topic={} key={} reason=PRODUCER_ERROR", record.topic(), record.key());
    }
}
