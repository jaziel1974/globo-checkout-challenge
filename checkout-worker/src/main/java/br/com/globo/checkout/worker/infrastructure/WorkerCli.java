package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/** Operational commands run in a short-lived worker process without listener or recovery scheduling. */
@Component
@org.springframework.context.annotation.Profile("worker-cli")
public final class WorkerCli {
    private final PaymentPorts.AttemptStore attempts;
    private final PublicationCoordinator publications;
    private final ObjectMapper mapper;
    private final String bootstrapServers;
    private final String commandTopic;
    private final String failureTopic;

    @org.springframework.beans.factory.annotation.Autowired
    public WorkerCli(PaymentPorts.AttemptStore attempts, PublicationCoordinator publications, ObjectMapper mapper,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${checkout.kafka.command-topic:checkout-v2-commands}") String commandTopic,
            @Value("${checkout.kafka.failure-topic:checkout-v2-failures}") String failureTopic) {
        this.attempts = attempts;
        this.publications = publications;
        this.mapper = mapper;
        this.bootstrapServers = bootstrapServers;
        this.commandTopic = commandTopic;
        this.failureTopic = failureTopic;
    }

    public int execute(String[] args) {
        if (args.length == 0) return usage();
        try {
            return switch (args[0]) {
                case "inspect" -> inspect(option(args, "--attempt-id"));
                case "authorize-publication" -> authorizePublication(option(args, "--attempt-id"));
                case "authorize-processing" -> authorizeProcessing(option(args, "--attempt-id"));
                case "mark-intervention" -> markIntervention(option(args, "--attempt-id"),
                    Integer.parseInt(option(args, "--failure-partition")), Long.parseLong(option(args, "--failure-offset")));
                default -> usage();
            };
        } catch (Exception ignored) {
            System.err.println("worker-cli failed reason=OPERATION_FAILED");
            return 1;
        }
    }

    private int inspect(String attemptId) {
        var snapshot = attempts.find(attemptId).orElse(null);
        if (snapshot == null) return fail("ATTEMPT_NOT_FOUND");
        var lease = attempts.inspectLease(attemptId);
        System.out.printf("attemptId=%s cartId=%s state=%s publicationCount=%d executionCount=%d nextPublicationAt=%d nextExecutionAt=%d leaseOwned=%s leaseExpiresAt=%d safeReason=%s%n",
            snapshot.attemptId(), snapshot.cartId(), snapshot.state(), snapshot.publicationCount(), snapshot.executionCount(),
            snapshot.nextPublicationAtMillis(), snapshot.nextExecutionAtMillis(), lease.owned(), lease.expiresAtMillis(),
            snapshot.safeReason() == null ? "" : snapshot.safeReason());
        return 0;
    }

    private int authorizePublication(String attemptId) {
        if (!attempts.authorizePublicationCycle(attemptId)) return fail("AUTHORIZATION_REJECTED");
        var before = attempts.find(attemptId).orElse(null);
        if (before == null) return fail("ATTEMPT_NOT_FOUND");
        if ("INTERVENCAO".equals(before.state())) {
            System.out.println("publicationBudgetAuthorized=true processingAuthorizationRequired=true");
            return 0;
        }
        return publishAuthorized(attemptId);
    }

    private int authorizeProcessing(String attemptId) {
        if (!attempts.authorizeProcessingCycle(attemptId)) return fail("PROCESSING_AUTHORIZATION_REJECTED; check publication budget authorization");
        var result = publications.publish(attemptId, Duration.ofSeconds(2));
        if (result == PublicationCoordinator.Result.NOT_DUE) {
            var scheduled = attempts.find(attemptId).orElse(null);
            if (scheduled == null) return fail("ATTEMPT_NOT_FOUND");
            System.out.printf("processingCycleAuthorized=true commandPublication=SCHEDULED nextPublicationAt=%d%n",
                scheduled.nextPublicationAtMillis());
            return 0;
        }
        return reportPublication(result);
    }

    private int publish(String attemptId) {
        return reportPublication(publications.publish(attemptId, Duration.ofSeconds(2)));
    }

    private int publishAuthorized(String attemptId) {
        return reportPublication(publications.publish(attemptId, Duration.ofSeconds(2)));
    }

    private int reportPublication(PublicationCoordinator.Result result) {
        if (result == PublicationCoordinator.Result.CONFIRMED || result == PublicationCoordinator.Result.UNKNOWN) {
            System.out.println("commandPublication=" + result.name());
            return 0;
        }
        return fail("PUBLICATION_" + result.name());
    }

    private int markIntervention(String attemptId, int partition, long offset) {
        if (!isUuid(attemptId) || partition < 0 || offset < 0) return fail("INVALID_FAILURE_REFERENCE");
        String raw = readFailure(partition, offset);
        if (raw == null) return fail("FAILURE_RECORD_NOT_FOUND");
        var failure = mapper.readTree(raw);
        if (failure == null || !failure.isObject() || !number(failure, "schemaVersion", 1)
            || !attemptId.equals(text(failure, "attemptId"))
            || !"REDIS_UNAVAILABLE".equals(text(failure, "reasonCode"))
            || !failureTopicCommand().equals(text(failure, "sourceTopic"))
            || !nonnegativeNumber(failure, "sourcePartition") || !nonnegativeNumber(failure, "sourceOffset"))
            return fail("FAILURE_RECORD_NOT_AUTHORIZED");
        var existing = attempts.find(attemptId).orElse(null);
        if (existing == null) return fail("ATTEMPT_NOT_FOUND");
        if ("APROVADO".equals(existing.state()) || "RECUSADO".equals(existing.state())) return fail("ATTEMPT_TERMINAL");
        if (!existing.cartId().equals(text(failure, "cartId"))) return fail("FAILURE_RECORD_NOT_AUTHORIZED");
        if (!"INTERVENCAO".equals(existing.state()) || !"REDIS_UNAVAILABLE".equals(existing.safeReason()))
            return fail("FAILURE_ALREADY_RECONCILED_OR_AUTHORIZED");
        System.out.println("interventionRecorded=true reason=REDIS_UNAVAILABLE");
        return 0;
    }

    private String readFailure(int partition, long offset) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "checkout-v2-worker-cli-" + UUID.randomUUID());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            var tp = new TopicPartition(failureTopic, partition);
            consumer.assign(java.util.List.of(tp));
            consumer.seek(tp, offset);
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (record.partition() == partition && record.offset() == offset) return record.value();
                    if (record.offset() > offset) return null;
                }
            }
            return null;
        }
    }

    private static String text(tools.jackson.databind.JsonNode node, String field) {
        var value = node.get(field);
        return value != null && value.isTextual() ? value.asText() : null;
    }
    private String failureTopicCommand() { return commandTopic; }
    private static boolean number(tools.jackson.databind.JsonNode node, String field, long expected) {
        var value = node.get(field); return value != null && value.isIntegralNumber() && value.asLong() == expected;
    }
    private static boolean nonnegativeNumber(tools.jackson.databind.JsonNode node, String field) {
        var value = node.get(field); return value != null && value.isIntegralNumber() && value.asLong() >= 0;
    }
    private static boolean isUuid(String value) { try { UUID.fromString(value); return true; } catch (RuntimeException e) { return false; } }
    private static String option(String[] args, String name) {
        for (int i = 1; i + 1 < args.length; i++) if (name.equals(args[i])) return args[i + 1];
        throw new IllegalArgumentException("missing option");
    }
    private static int fail(String reason) { System.err.println("worker-cli failed reason=" + reason); return 1; }
    private static int usage() {
        System.err.println("usage: worker inspect|authorize-publication|authorize-processing --attempt-id <uuid> | mark-intervention --attempt-id <uuid> --failure-partition <n> --failure-offset <n>");
        return 2;
    }
}
