package br.com.globo.checkout.worker.application;

import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentBusinessException;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Validates a Kafka record, owns its attempt lease and returns whether its offset is safe to acknowledge. */
@Service
public class PaymentCommandHandler {
    private static final Set<String> BASE_FIELDS = Set.of("schemaVersion", "attemptId", "cartId", "paymentMethod");
    private static final Set<String> CARD_FIELDS = Set.of("schemaVersion", "attemptId", "cartId", "paymentMethod", "lastDigits");
    private static final Set<String> PROCESSABLE_STATES = Set.of("AGUARDANDO_PUBLICACAO", "PUBLICADO", "PROCESSANDO");
    private final ObjectMapper mapper;
    private final PaymentPorts.AttemptStore attempts;
    private final PaymentProcessor processor;
    private final ScheduledLeaseRenewer renewer;

    @Autowired
    public PaymentCommandHandler(ObjectMapper mapper, PaymentPorts.AttemptStore attempts,
                                 PaymentProcessor processor, ScheduledLeaseRenewer renewer) {
        this.mapper = mapper;
        this.attempts = attempts;
        this.processor = processor;
        this.renewer = renewer;
    }

    public Outcome handle(String key, String rawValue) {
        return handleDetailed(key, rawValue).outcome();
    }

    public HandlingResult handleDetailed(String key, String rawValue) {
        PaymentCommand command = parse(rawValue);
        if (command == null) return new HandlingResult(Outcome.INVALID, null, null, "INVALID_COMMAND");
        if (key == null || !key.equals(command.cartId())) return result(Outcome.INVALID, command, "INVALID_COMMAND");

        final PaymentPorts.AttemptSnapshot known;
        try {
            known = attempts.find(command.attemptId()).orElse(null);
        } catch (RuntimeException e) {
            return result(Outcome.REDIS_UNAVAILABLE, command, "REDIS_UNAVAILABLE");
        }
        if (known == null) return result(Outcome.MISSING_STATE, command, "ATTEMPT_STATE_MISSING");
        if (known.command() == null || known.cartId() == null || known.state() == null) return result(Outcome.MISSING_STATE, command, "ATTEMPT_STATE_CORRUPT");
        if (!known.command().equals(command) || !known.cartId().equals(command.cartId())) return result(Outcome.INVALID, command, "COMMAND_MISMATCH");
        if (terminal(known.state())) return known.result() == null ? result(Outcome.MISSING_STATE, command, "INCOMPLETE_TERMINAL_STATE") : result(Outcome.ACKNOWLEDGE, command, null);
        if ("INTERVENCAO".equals(known.state())) return result(Outcome.INTERVENTION, command, safeReason(known.safeReason()));
        if (!PROCESSABLE_STATES.contains(known.state())) return result(Outcome.MISSING_STATE, command, "ATTEMPT_STATE_CORRUPT");

        final PaymentPorts.ProcessingLease lease;
        try {
            lease = attempts.acquire(command.attemptId()).orElse(null);
        } catch (RuntimeException e) {
            return result(Outcome.REDIS_UNAVAILABLE, command, "REDIS_UNAVAILABLE");
        }
        if (lease == null) {
            try {
                var latest = attempts.find(command.attemptId()).orElse(null);
                if (latest != null && terminal(latest.state())) return latest.result() == null ? result(Outcome.MISSING_STATE, command, "INCOMPLETE_TERMINAL_STATE") : result(Outcome.ACKNOWLEDGE, command, null);
                if (latest != null && "INTERVENCAO".equals(latest.state())) return result(Outcome.INTERVENTION, command, safeReason(latest.safeReason()));
            } catch (RuntimeException ignored) {
                return result(Outcome.REDIS_UNAVAILABLE, command, "REDIS_UNAVAILABLE");
            }
            return result(Outcome.RETRY, command, "PROCESSING_NOT_DUE");
        }

        try (ScheduledLeaseRenewer.Guard guard = renewer.start(lease)) {
            processor.process(command, lease);
            if (!guard.isOwned()) return terminalResult(command.attemptId()) ? result(Outcome.ACKNOWLEDGE, command, null) : result(Outcome.RETRY, command, "OWNERSHIP_LOST");
            return terminalResult(command.attemptId()) ? result(Outcome.ACKNOWLEDGE, command, null) : result(Outcome.RETRY, command, "RESULT_NOT_CONFIRMED");
        } catch (RuntimeException e) {
            try {
                var latest = attempts.find(command.attemptId()).orElse(null);
                if (latest != null && terminal(latest.state()) && latest.result() != null) return result(Outcome.ACKNOWLEDGE, command, null);
                if (latest != null && "INTERVENCAO".equals(latest.state())) return result(Outcome.INTERVENTION, command, safeReason(latest.safeReason()));
                if (e instanceof PaymentBusinessException business
                    && business.kind() == PaymentBusinessException.Kind.CART_NOT_FOUND) {
                    if (attempts.markOwnedIntervention(lease, "CART_SNAPSHOT_MISSING"))
                        return result(Outcome.INTERVENTION, command, "CART_SNAPSHOT_MISSING");
                    return result(Outcome.RETRY, command, "OWNERSHIP_LOST");
                }
                if (latest != null && latest.executionCount() >= 3) {
                    if (attempts.markExecutionExhausted(lease))
                        return result(Outcome.INTERVENTION, command, "EXECUTION_EXHAUSTED");
                    var after = attempts.find(command.attemptId()).orElse(null);
                    if (after != null && terminal(after.state()) && after.result() != null) return result(Outcome.ACKNOWLEDGE, command, null);
                    if (after != null && "INTERVENCAO".equals(after.state())) return result(Outcome.INTERVENTION, command, safeReason(after.safeReason()));
                }
                if (attempts.releaseForRetry(lease)) return result(Outcome.RETRY, command, "PROCESSING_FAILED");
                return result(Outcome.RETRY, command, "OWNERSHIP_LOST");
            } catch (RuntimeException redisFailure) {
                return result(Outcome.REDIS_UNAVAILABLE, command, "REDIS_UNAVAILABLE");
            }
        }
    }

    /** Persist a fail-closed intervention before a Redis-outage record may be forwarded to the DLT. */
    public RedisFailureAttempt resolveRedisFailure(String key, String rawValue) {
        PaymentCommand command = parse(rawValue);
        if (command == null) return new RedisFailureAttempt(RedisFailureResolution.MISSING_STATE, null, null, "INVALID_COMMAND");
        if (key == null || !key.equals(command.cartId()))
            return new RedisFailureAttempt(RedisFailureResolution.MISMATCH, command.attemptId(), command.cartId(), "COMMAND_MISMATCH");
        var current = attempts.find(command.attemptId()).orElse(null);
        if (current == null) return new RedisFailureAttempt(RedisFailureResolution.MISSING_STATE, command.attemptId(), command.cartId(), "ATTEMPT_STATE_MISSING");
        if (!command.equals(current.command()) || !command.cartId().equals(current.cartId()))
            return new RedisFailureAttempt(RedisFailureResolution.MISMATCH, command.attemptId(), command.cartId(), "COMMAND_MISMATCH");
        if (terminal(current.state())) return new RedisFailureAttempt(current.result() == null
            ? RedisFailureResolution.MISSING_STATE : RedisFailureResolution.TERMINAL, command.attemptId(), command.cartId(),
            current.result() == null ? "INCOMPLETE_TERMINAL_STATE" : null);
        if ("INTERVENCAO".equals(current.state())) return new RedisFailureAttempt(RedisFailureResolution.ALREADY_BLOCKED,
            command.attemptId(), command.cartId(), safeReason(current.safeReason()));
        boolean persisted = attempts.markIntervention(command.attemptId(), "REDIS_UNAVAILABLE");
        if (persisted) return new RedisFailureAttempt(RedisFailureResolution.PERSISTED, command.attemptId(), command.cartId(), "REDIS_UNAVAILABLE");
        var latest = attempts.find(command.attemptId()).orElse(null);
        if (latest != null && terminal(latest.state()) && latest.result() != null)
            return new RedisFailureAttempt(RedisFailureResolution.TERMINAL, command.attemptId(), command.cartId(), null);
        if (latest != null && "INTERVENCAO".equals(latest.state()))
            return new RedisFailureAttempt(RedisFailureResolution.ALREADY_BLOCKED, command.attemptId(), command.cartId(), safeReason(latest.safeReason()));
        return new RedisFailureAttempt(RedisFailureResolution.UNRESOLVED, command.attemptId(), command.cartId(), null);
    }

    private PaymentCommand parse(String rawValue) {
        try {
            JsonNode node = mapper.readTree(rawValue);
            if (node == null || !node.isObject()) return null;
            var names = new java.util.HashSet<>(node.propertyNames());
            JsonNode version = node.get("schemaVersion");
            JsonNode attemptId = node.get("attemptId");
            JsonNode cartId = node.get("cartId");
            JsonNode method = node.get("paymentMethod");
            JsonNode lastDigits = node.get("lastDigits");
            if (version == null || !version.isIntegralNumber() || !version.canConvertToInt()
                || attemptId == null || !attemptId.isTextual()
                || cartId == null || !cartId.isTextual()
                || method == null || !method.isTextual()) return null;
            String paymentMethod = method.asText();
            if (PaymentCommand.PIX.equals(paymentMethod)) {
                if (!BASE_FIELDS.equals(names)) return null;
            } else if (PaymentCommand.CREDIT.equals(paymentMethod) || PaymentCommand.DEBIT.equals(paymentMethod)) {
                if (!CARD_FIELDS.equals(names) || lastDigits == null || !lastDigits.isTextual()) return null;
            } else return null;
            return mapper.treeToValue(node, PaymentCommand.class);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean terminalResult(String attemptId) {
        try { return attempts.find(attemptId).map(snapshot -> terminal(snapshot.state()) && snapshot.result() != null).orElse(false); }
        catch (RuntimeException e) { return false; }
    }
    private static boolean terminal(String state) { return "APROVADO".equals(state) || "RECUSADO".equals(state); }

    private static String safeReason(String value) { return value != null && value.matches("[A-Z0-9_]{1,64}") ? value : "INTERVENTION_REQUIRED"; }
    private static HandlingResult result(Outcome outcome, PaymentCommand command, String reason) {
        return new HandlingResult(outcome, command.attemptId(), command.cartId(), reason);
    }
    public enum Outcome { ACKNOWLEDGE, RETRY, INVALID, MISSING_STATE, INTERVENTION, REDIS_UNAVAILABLE }
    public enum RedisFailureResolution { PERSISTED, ALREADY_BLOCKED, TERMINAL, MISSING_STATE, MISMATCH, UNRESOLVED }
    public record RedisFailureAttempt(RedisFailureResolution resolution, String attemptId, String cartId, String reasonCode) {}
    public record HandlingResult(Outcome outcome, String attemptId, String cartId, String reasonCode) {}
}
