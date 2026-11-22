package br.com.globo.checkout.bff.infrastructure;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
import br.com.globo.checkout.worker.infrastructure.KafkaCommandPublisher;
import br.com.globo.checkout.worker.infrastructure.RedisAttemptStore;
import br.com.globo.checkout.worker.infrastructure.SafeKafkaProducerListener;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.databind.ObjectMapper;

/** Explicit v2 beans: no component scan of worker listeners or recovery scheduler. */
@Configuration
public class CheckoutV2Configuration {
    @Bean PaymentPorts.AttemptStore v2AttemptStore(StringRedisTemplate redis, ObjectMapper mapper) {
        return new RedisAttemptStore(redis, mapper);
    }

    @Bean(destroyMethod = "shutdownNow") ThreadPoolExecutor v2OperationExecutor() {
        return new ThreadPoolExecutor(16, 16, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "checkout-v2-http-dependency");
            thread.setDaemon(true);
            return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean(destroyMethod = "shutdownNow") ThreadPoolExecutor v2KafkaSendExecutor() {
        return new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "checkout-v2-http-kafka-send");
            thread.setDaemon(true);
            return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean(destroyMethod = "destroy") DefaultKafkaProducerFactory<String, String> v2ProducerFactory(
            @Value("${checkout.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${checkout.kafka.producer.acks:all}") String acks,
            @Value("${checkout.kafka.producer.enable-idempotence:true}") boolean idempotence,
            @Value("${checkout.kafka.producer.linger-ms:0}") int lingerMs,
            @Value("${checkout.kafka.producer.request-timeout-ms:1500}") int requestTimeoutMs,
            @Value("${checkout.kafka.producer.delivery-timeout-ms:2000}") int deliveryTimeoutMs,
            @Value("${checkout.kafka.producer.max-block-ms:2000}") int maxBlockMs) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, acks);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean(name = "v2KafkaTemplate") KafkaTemplate<String, String> v2KafkaTemplate(
            ProducerFactory<String, String> v2ProducerFactory) {
        KafkaTemplate<String, String> kafka = new KafkaTemplate<>(v2ProducerFactory);
        kafka.setProducerListener(new SafeKafkaProducerListener());
        return kafka;
    }

    @Bean PaymentPorts.CommandPublisher v2CommandPublisher(@org.springframework.beans.factory.annotation.Qualifier("v2KafkaTemplate") KafkaTemplate<String, String> kafka,
            ObjectMapper mapper, ThreadPoolExecutor v2KafkaSendExecutor,
            @Value("${checkout.kafka.command-topic:checkout-v2-commands}") String topic) {
        return new KafkaCommandPublisher(kafka, mapper, v2KafkaSendExecutor, topic);
    }

    @Bean PublicationCoordinator v2PublicationCoordinator(PaymentPorts.AttemptStore attempts,
            PaymentPorts.CommandPublisher v2CommandPublisher) {
        return new PublicationCoordinator(attempts, v2CommandPublisher);
    }
}
