package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Testcontainers(disabledWithoutDocker = true)
class KafkaCommandPublisherIntegrationTest {
    private static final String TOPIC = "checkout-v2-commands";
    @Container static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"))
        .withEnv("KAFKA_OPTS", "-XX:-UseBASE64Intrinsics");

    @Test void publishesVersionedMinimalCommandAndAcknowledgesDuplicateRecords() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }

        var producerFactory = new DefaultKafkaProducerFactory<String, String>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.LINGER_MS_CONFIG, 0,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1500,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        var kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setProducerListener(new SafeKafkaProducerListener());
        var executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "test-kafka-send"); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
        try {
            PaymentCommand command = new PaymentCommand(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                PaymentCommand.CREDIT, "4242");
            KafkaCommandPublisher publisher = new KafkaCommandPublisher(kafkaTemplate, new ObjectMapper(), executor, TOPIC);
            assertEquals(PaymentPorts.PublishResult.CONFIRMED, publisher.publish(command, Duration.ofSeconds(2)));
            assertEquals(PaymentPorts.PublishResult.CONFIRMED, publisher.publish(command, Duration.ofSeconds(2)));

            var consumerProps = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "publisher-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(java.util.List.of(TOPIC));
                var records = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (records.size() < 2 && System.nanoTime() < until) {
                    consumer.poll(Duration.ofMillis(250)).forEach(records::add);
                }
                assertEquals(2, records.size());
                for (var record : records) {
                    assertEquals(command.cartId(), record.key());
                    JsonNode value = new ObjectMapper().readTree(record.value());
                    assertEquals(1, value.get("schemaVersion").asInt());
                    assertEquals(command.attemptId(), value.get("attemptId").asText());
                    assertEquals(command.cartId(), value.get("cartId").asText());
                    assertEquals(command.paymentMethod(), value.get("paymentMethod").asText());
                    assertEquals("4242", value.get("lastDigits").asText());
                    assertFalse(record.value().toLowerCase().contains("pan"));
                    assertFalse(record.value().toLowerCase().contains("cvv"));
                    assertEquals(5, value.size());
                }
            }

            // The broker may append a record even if the caller loses the acknowledgement.
            var ackLost = new java.util.concurrent.CompletableFuture<org.springframework.kafka.support.SendResult<String, String>>();
            KafkaTemplate<String, String> ackLostTemplate = org.mockito.Mockito.spy(kafkaTemplate);
            org.mockito.Mockito.doAnswer(call -> {
                kafkaTemplate.send(TOPIC, command.cartId(), new ObjectMapper().writeValueAsString(command));
                return ackLost;
            }).when(ackLostTemplate).send(org.mockito.ArgumentMatchers.eq(TOPIC),
                org.mockito.ArgumentMatchers.eq(command.cartId()), org.mockito.ArgumentMatchers.anyString());
            KafkaCommandPublisher uncertainPublisher = new KafkaCommandPublisher(ackLostTemplate, new ObjectMapper(), executor, TOPIC);
            assertEquals(PaymentPorts.PublishResult.UNKNOWN, uncertainPublisher.publish(command, Duration.ofMillis(200)));
            assertFalse(ackLost.isCancelled());
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ConsumerConfig.GROUP_ID_CONFIG, "ack-lost-test-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
                consumer.subscribe(java.util.List.of(TOPIC));
                var records = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (records.size() < 3 && System.nanoTime() < until) {
                    consumer.poll(Duration.ofMillis(250)).forEach(records::add);
                }
                assertEquals(3, records.size());
                assertEquals(3, records.stream().filter(record -> record.key().equals(command.cartId())).count());
            }

            // A stopped real broker is also an unknown outcome; it does not authorize a blind new send.
            kafka.stop();
            var unavailablePublisher = new KafkaCommandPublisher(kafkaTemplate, new ObjectMapper(), executor, TOPIC);
            long unavailableStarted = System.nanoTime();
            assertEquals(PaymentPorts.PublishResult.UNKNOWN, unavailablePublisher.publish(command, Duration.ofSeconds(2)));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - unavailableStarted) < 2500);
        } finally {
            executor.shutdownNow();
            kafkaTemplate.destroy();
            producerFactory.destroy();
        }
    }
}
