package br.com.globo.checkout.worker.infrastructure;

import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

/** Redis-backed v2 attempt state. Every multi-key state transition is performed by a single Lua script. */
@Repository
public class RedisAttemptStore implements PaymentPorts.AttemptStore {
    private static final Logger log = LoggerFactory.getLogger(RedisAttemptStore.class);
    private static final String CART_PREFIX = "checkout:v2:cart:";
    private static final String ATTEMPT_PREFIX = "checkout:v2:attempt:";
    private static final String PUBLICATION_DUE = "checkout:v2:publication-due";
    private static final String FIND_PUBLICATION_DUE = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        return redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now(),'LIMIT',0,ARGV[1])
        """;
    private static final String CREATE = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('EXISTS',KEYS[1]) == 0 or redis.call('HEXISTS',KEYS[1],'payload') == 0 then return 0 end
        local approved=redis.call('HGET',KEYS[2],'approvedAttemptId'); if approved then return 2 end
        if redis.call('EXISTS',KEYS[3]) == 1 then return 4 end
        local active=redis.call('HGET',KEYS[2],'activeAttemptId')
        if active then
          local activeKey=ARGV[1]..active
          local state=redis.call('HGET',activeKey,'state')
          if not state or redis.call('HGET',activeKey,'cartId') ~= ARGV[3] then return 3 end
          if state == 'APROVADO' then return 2 end
          if state ~= 'RECUSADO' then return 3 end
        end
        local ts=now()
        redis.call('HSET',KEYS[2],'activeAttemptId',ARGV[2])
        redis.call('HSET',KEYS[3],'attemptId',ARGV[2],'cartId',ARGV[3],'command',ARGV[4],
          'state','AGUARDANDO_PUBLICACAO','publicationConfirmed','false','publicationCount','0',
          'executionCount','0','executionCycle','1','nextPublicationAt',string.format('%.0f',ts),
          'nextExecutionAt','0','leaseToken','','leaseUntil','0','safeReason','')
        -- The initial BFF send is eligible immediately via nextPublicationAt, but crash recovery
        -- must wait the same 30-second publication interval before discovering this reservation.
        redis.call('ZADD',KEYS[4],ts+30000,ARGV[2])
        return 1
        """;
    private static final String RESERVE_PUBLICATION = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('EXISTS',KEYS[1]) == 0 then return 0 end
        local state=redis.call('HGET',KEYS[1],'state')
        if state ~= 'AGUARDANDO_PUBLICACAO' or redis.call('HGET',KEYS[1],'publicationConfirmed') == 'true' then return 1 end
        local ts=now(); local due=tonumber(redis.call('HGET',KEYS[1],'nextPublicationAt') or '0')
        if ts < due then return 2 end
        local count=tonumber(redis.call('HGET',KEYS[1],'publicationCount') or '0')
        if count >= 3 then
          redis.call('HSET',KEYS[1],'state','INTERVENCAO','safeReason','PUBLICATION_EXHAUSTED','leaseToken','','leaseUntil','0')
          redis.call('ZREM',KEYS[2],ARGV[1]); return 3
        end
        count=count+1
        redis.call('HSET',KEYS[1],'publicationCount',tostring(count),'nextPublicationAt',string.format('%.0f',ts+30000))
        redis.call('ZADD',KEYS[2],ts+30000,ARGV[1])
        return 4
        """;
    private static final String CONFIRM_PUBLICATION = """
        if redis.call('EXISTS',KEYS[1]) == 0 then return 0 end
        redis.call('HSET',KEYS[1],'publicationConfirmed','true')
        local state=redis.call('HGET',KEYS[1],'state')
        if state == 'AGUARDANDO_PUBLICACAO' then redis.call('HSET',KEYS[1],'state','PUBLICADO') end
        redis.call('ZREM',KEYS[2],ARGV[1]); return 1
        """;
    private static final String ACQUIRE = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('EXISTS',KEYS[1]) == 0 then return 0 end
        local state=redis.call('HGET',KEYS[1],'state')
        if state == 'APROVADO' or state == 'RECUSADO' then return 2 end
        if state == 'INTERVENCAO' then return 3 end
        local ts=now(); local untilAt=tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0')
        if untilAt > ts then return 4 end
        if state ~= 'AGUARDANDO_PUBLICACAO' and state ~= 'PUBLICADO' and state ~= 'PROCESSANDO' then return 5 end
        local due=tonumber(redis.call('HGET',KEYS[1],'nextExecutionAt') or '0'); if ts < due then return 5 end
        redis.call('HSET',KEYS[1],'state','PROCESSANDO','leaseToken',ARGV[2],'leaseUntil',string.format('%.0f',ts+30000))
        redis.call('ZREM',KEYS[2],ARGV[1])
        return 1
        """;
    private static final String RELEASE_FOR_RETRY = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'PROCESSANDO' or redis.call('HGET',KEYS[1],'leaseToken') ~= ARGV[1] then return 0 end
        if tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0') <= now() then return 0 end
        if redis.call('HGET',KEYS[2],'activeAttemptId') ~= ARGV[2] then return 0 end
        redis.call('HSET',KEYS[1],'state','PUBLICADO','leaseToken','','leaseUntil','0')
        return 1
        """;
    private static final String RENEW = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'PROCESSANDO' or redis.call('HGET',KEYS[1],'leaseToken') ~= ARGV[1] then return 0 end
        local ts=now(); if tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0') <= ts then return 0 end
        redis.call('HSET',KEYS[1],'leaseUntil',string.format('%.0f',ts+30000)); return 1
        """;
    private static final String RESERVE_EXECUTION = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'PROCESSANDO' or redis.call('HGET',KEYS[1],'leaseToken') ~= ARGV[1] then return -1 end
        local ts=now(); if tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0') <= ts then return -1 end
        local count=tonumber(redis.call('HGET',KEYS[1],'executionCount') or '0')
        if count >= 3 then return -2 end
        local due=tonumber(redis.call('HGET',KEYS[1],'nextExecutionAt') or '0'); if ts < due then return 0 end
        count=count+1
        redis.call('HSET',KEYS[1],'executionCount',tostring(count),'nextExecutionAt',string.format('%.0f',ts+1000))
        return count
        """;
    private static final String COMPLETE = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        local currentState=redis.call('HGET',KEYS[1],'state')
        if currentState == 'APROVADO' or currentState == 'RECUSADO' then return 2 end
        if currentState ~= 'PROCESSANDO' or redis.call('HGET',KEYS[1],'leaseToken') ~= ARGV[1] then return 0 end
        if tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0') <= now() then return 0 end
        if redis.call('HGET',KEYS[1],'cartId') ~= ARGV[5] then return 0 end
        if redis.call('HGET',KEYS[2],'activeAttemptId') ~= ARGV[4] then return 0 end
        local outcome=ARGV[3]
        if outcome ~= 'APROVADO' and outcome ~= 'RECUSADO' then return 0 end
        redis.call('HSET',KEYS[1],'state',outcome,'result',ARGV[2],'leaseToken','','leaseUntil','0','safeReason','')
        redis.call('HDEL',KEYS[2],'activeAttemptId')
        if outcome == 'APROVADO' then redis.call('HSET',KEYS[2],'approvedAttemptId',ARGV[4]) end
        redis.call('ZREM',KEYS[3],ARGV[4]); return 1
        """;
    private static final String MARK_INTERVENTION = """
        local state=redis.call('HGET',KEYS[1],'state')
        if not state or state == 'APROVADO' or state == 'RECUSADO' then return 0 end
        local cartId=redis.call('HGET',KEYS[1],'cartId'); if not cartId or redis.call('HGET','checkout:v2:cart:'..cartId,'activeAttemptId') ~= ARGV[1] then return 0 end
        redis.call('HSET',KEYS[1],'state','INTERVENCAO','safeReason',ARGV[2],'leaseToken','','leaseUntil','0')
        redis.call('ZREM',KEYS[2],ARGV[1]); return 1
        """;
    private static final String MARK_EXECUTION_EXHAUSTED = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'PROCESSANDO' or redis.call('HGET',KEYS[1],'leaseToken') ~= ARGV[1] then return 0 end
        if tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0') <= now() then return 0 end
        if tonumber(redis.call('HGET',KEYS[1],'executionCount') or '0') < 3 then return 0 end
        if redis.call('HGET',KEYS[2],'activeAttemptId') ~= ARGV[2] or redis.call('HEXISTS',KEYS[1],'result') == 1 then return 0 end
        redis.call('HSET',KEYS[1],'state','INTERVENCAO','safeReason','EXECUTION_EXHAUSTED','leaseToken','','leaseUntil','0')
        redis.call('ZREM',KEYS[3],ARGV[2]); return 1
        """;
    private static final String MARK_OWNED_INTERVENTION = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'PROCESSANDO' or redis.call('HGET',KEYS[1],'leaseToken') ~= ARGV[1] then return 0 end
        if tonumber(redis.call('HGET',KEYS[1],'leaseUntil') or '0') <= now() then return 0 end
        if redis.call('HGET',KEYS[2],'activeAttemptId') ~= ARGV[2] or redis.call('HEXISTS',KEYS[1],'result') == 1 then return 0 end
        redis.call('HSET',KEYS[1],'state','INTERVENCAO','safeReason',ARGV[3],'leaseToken','','leaseUntil','0')
        redis.call('ZREM',KEYS[3],ARGV[2]); return 1
        """;
    private static final String MARK_PUBLICATION_EXHAUSTED = """
        local state=redis.call('HGET',KEYS[1],'state')
        if not state or state == 'APROVADO' or state == 'RECUSADO' then return 0 end
        local cartId=redis.call('HGET',KEYS[1],'cartId'); if not cartId or redis.call('HGET','checkout:v2:cart:'..cartId,'activeAttemptId') ~= ARGV[1] then return 0 end
        if redis.call('HGET',KEYS[1],'publicationConfirmed') == 'true' then return 0 end
        if tonumber(redis.call('HGET',KEYS[1],'publicationCount') or '0') < 3 then return 0 end
        redis.call('HSET',KEYS[1],'state','INTERVENCAO','safeReason','PUBLICATION_EXHAUSTED','leaseToken','','leaseUntil','0')
        redis.call('ZREM',KEYS[2],ARGV[1]); return 1
        """;
    private static final String AUTHORIZE_PUBLICATION = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'INTERVENCAO' or redis.call('HEXISTS',KEYS[1],'result') == 1 then return 0 end
        local cartId=redis.call('HGET',KEYS[1],'cartId'); if not cartId or redis.call('HGET','checkout:v2:cart:'..cartId,'activeAttemptId') ~= ARGV[1] then return 0 end
        local reason=redis.call('HGET',KEYS[1],'safeReason') or ''
        redis.call('HSET',KEYS[1],'publicationCount','0')
        if reason == 'EXECUTION_EXHAUSTED' then
          redis.call('HSET',KEYS[1],'publicationCycleAuthorized','true'); return 1
        end
        local ts=now()
        redis.call('HSET',KEYS[1],'publicationConfirmed','false','nextPublicationAt',string.format('%.0f',ts),'safeReason','','state','AGUARDANDO_PUBLICACAO')
        redis.call('ZADD',KEYS[2],ts,ARGV[1]); return 1
        """;
    private static final String AUTHORIZE_PROCESSING = """
        local function now() local t=redis.call('TIME'); return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
        if redis.call('HGET',KEYS[1],'state') ~= 'INTERVENCAO' or redis.call('HEXISTS',KEYS[1],'result') == 1 then return 0 end
        local cartId=redis.call('HGET',KEYS[1],'cartId'); if not cartId or redis.call('HGET','checkout:v2:cart:'..cartId,'activeAttemptId') ~= ARGV[1] then return 0 end
        local pubCount=tonumber(redis.call('HGET',KEYS[1],'publicationCount') or '0')
        local authorized=redis.call('HGET',KEYS[1],'publicationCycleAuthorized') == 'true'
        if pubCount >= 3 and not authorized then return -1 end
        if authorized then redis.call('HDEL',KEYS[1],'publicationCycleAuthorized') end
        local ts=now()
        local nextPublication=tonumber(redis.call('HGET',KEYS[1],'nextPublicationAt') or '0')
        if nextPublication < ts then nextPublication=ts end
        redis.call('HSET',KEYS[1],'state','AGUARDANDO_PUBLICACAO','safeReason','','executionCount','0',
          'publicationConfirmed','false','nextPublicationAt',string.format('%.0f',nextPublication),'nextExecutionAt','0','leaseToken','','leaseUntil','0')
        redis.call('ZADD',KEYS[2],nextPublication,ARGV[1]); return 1
        """;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public RedisAttemptStore(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    @Override public PaymentPorts.ReservationResult create(PaymentCommand command) {
        try {
            Long result = run(CREATE, List.of("checkout:cart:" + command.cartId(), cartKey(command.cartId()), attemptKey(command.attemptId()), PUBLICATION_DUE),
                ATTEMPT_PREFIX, command.attemptId(), command.cartId(), mapper.writeValueAsString(command));
            if (result == null) return PaymentPorts.ReservationResult.UNKNOWN;
            return switch (result.intValue()) {
                case 1 -> PaymentPorts.ReservationResult.CREATED;
                case 2 -> PaymentPorts.ReservationResult.ALREADY_APPROVED;
                case 3 -> PaymentPorts.ReservationResult.ACTIVE_ATTEMPT;
                case 0 -> PaymentPorts.ReservationResult.CART_NOT_FOUND;
                default -> PaymentPorts.ReservationResult.UNKNOWN;
            };
        } catch (Exception e) { return PaymentPorts.ReservationResult.UNKNOWN; }
    }

    @Override public Optional<PaymentPorts.AttemptSnapshot> find(String attemptId) {
        Map<Object,Object> values = redis.opsForHash().entries(attemptKey(attemptId));
        if (values.isEmpty()) return Optional.empty();
        return Optional.of(snapshot(values));
    }

    @Override public Optional<PaymentPorts.AttemptSnapshot> findActiveByCart(String cartId) {
        Object active = redis.opsForHash().get(cartKey(cartId), "activeAttemptId");
        return active instanceof String attemptId ? find(attemptId) : Optional.empty();
    }

    @Override public boolean isApprovedByCart(String cartId) {
        return redis.opsForHash().hasKey(cartKey(cartId), "approvedAttemptId");
    }

    @Override public List<String> findPublicationDue(int limit) {
        int boundedLimit = Math.max(1, Math.min(limit, 100));
        @SuppressWarnings("unchecked")
        List<Object> ids = redis.execute(new DefaultRedisScript<>(FIND_PUBLICATION_DUE, List.class),
            List.of(PUBLICATION_DUE), Integer.toString(boundedLimit));
        if (ids == null) return List.of();
        return ids.stream().filter(String.class::isInstance).map(String.class::cast).toList();
    }

    @Override public PaymentPorts.PublishReservation reservePublication(String attemptId) {
        Long result = run(RESERVE_PUBLICATION, List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId);
        return publicationReservation(result);
    }

    private static PaymentPorts.PublishReservation publicationReservation(Long result) {
        if (result == null) return PaymentPorts.PublishReservation.UNKNOWN;
        return switch (result.intValue()) {
            case 4 -> PaymentPorts.PublishReservation.RESERVED;
            case 2 -> PaymentPorts.PublishReservation.NOT_DUE;
            case 3 -> PaymentPorts.PublishReservation.EXHAUSTED;
            case 1 -> PaymentPorts.PublishReservation.INELIGIBLE;
            case 5 -> PaymentPorts.PublishReservation.UNKNOWN;
            default -> PaymentPorts.PublishReservation.UNKNOWN;
        };
    }

    @Override public boolean confirmPublication(String attemptId) {
        return Long.valueOf(1).equals(run(CONFIRM_PUBLICATION, List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId));
    }

    @Override public boolean markPublicationExhausted(String attemptId) {
        return Long.valueOf(1).equals(run(MARK_PUBLICATION_EXHAUSTED, List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId));
    }

    @Override public Optional<PaymentPorts.ProcessingLease> acquire(String attemptId) {
        String token = UUID.randomUUID().toString();
        Long result = run(ACQUIRE, List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId, token);
        if (!Long.valueOf(1).equals(result)) return Optional.empty();
        Object expiresAt = redis.opsForHash().get(attemptKey(attemptId), "leaseUntil");
        if (!(expiresAt instanceof String value)) return Optional.empty();
        return Optional.of(new PaymentPorts.ProcessingLease(attemptId, token, Long.parseLong(value)));
    }

    @Override public boolean renew(PaymentPorts.ProcessingLease lease) {
        return Long.valueOf(1).equals(run(RENEW, List.of(attemptKey(lease.attemptId())), lease.ownerToken()));
    }

    @Override public boolean releaseForRetry(PaymentPorts.ProcessingLease lease) {
        Object cartId = redis.opsForHash().get(attemptKey(lease.attemptId()), "cartId");
        if (!(cartId instanceof String value)) return false;
        return Long.valueOf(1).equals(run(RELEASE_FOR_RETRY,
            List.of(attemptKey(lease.attemptId()), cartKey(value)), lease.ownerToken(), lease.attemptId()));
    }

    @Override public Optional<Integer> reserveExecution(PaymentPorts.ProcessingLease lease) {
        return reserveExecutionInternal(lease);
    }

    private Optional<Integer> reserveExecutionInternal(PaymentPorts.ProcessingLease lease) {
        Long result = run(RESERVE_EXECUTION, List.of(attemptKey(lease.attemptId())), lease.ownerToken());
        return result == null || result < 0 ? Optional.empty() : Optional.of(result.intValue());
    }

    @Override public PaymentPorts.CompletionResult complete(PaymentPorts.ProcessingLease lease, PaymentResult result) {
        return completeInternal(lease, result);
    }

    private PaymentPorts.CompletionResult completeInternal(PaymentPorts.ProcessingLease lease, PaymentResult result) {
        try {
            Long code = run(COMPLETE, List.of(attemptKey(lease.attemptId()), cartKey(result.cartId()), PUBLICATION_DUE),
                lease.ownerToken(), mapper.writeValueAsString(result), result.outcome() == PaymentResult.Outcome.APPROVED ? "APROVADO" : "RECUSADO",
                lease.attemptId(), result.cartId());
            if (code == null) return PaymentPorts.CompletionResult.UNKNOWN;
            return switch (code.intValue()) {
                case 1 -> PaymentPorts.CompletionResult.COMPLETED;
                case 2 -> PaymentPorts.CompletionResult.ALREADY_TERMINAL;
                case 3 -> PaymentPorts.CompletionResult.LOST_OWNERSHIP;
                default -> PaymentPorts.CompletionResult.LOST_OWNERSHIP;
            };
        } catch (Exception e) { return PaymentPorts.CompletionResult.UNKNOWN; }
    }

    @Override public boolean markIntervention(String attemptId, String safeReason) {
        if (safeReason == null || !safeReason.matches("[A-Z0-9_]{1,64}")) throw new IllegalArgumentException("safeReason must be a safe code");
        return Long.valueOf(1).equals(run(MARK_INTERVENTION, List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId, safeReason));
    }

    @Override public boolean markExecutionExhausted(PaymentPorts.ProcessingLease lease) {
        Object cartId = redis.opsForHash().get(attemptKey(lease.attemptId()), "cartId");
        if (!(cartId instanceof String value)) return false;
        return Long.valueOf(1).equals(run(MARK_EXECUTION_EXHAUSTED,
            List.of(attemptKey(lease.attemptId()), cartKey(value), PUBLICATION_DUE), lease.ownerToken(), lease.attemptId()));
    }

    @Override public boolean markOwnedIntervention(PaymentPorts.ProcessingLease lease, String safeReason) {
        if (safeReason == null || !safeReason.matches("[A-Z0-9_]{1,64}")) throw new IllegalArgumentException("safeReason must be a safe code");
        Object cartId = redis.opsForHash().get(attemptKey(lease.attemptId()), "cartId");
        if (!(cartId instanceof String value)) return false;
        return Long.valueOf(1).equals(run(MARK_OWNED_INTERVENTION,
            List.of(attemptKey(lease.attemptId()), cartKey(value), PUBLICATION_DUE), lease.ownerToken(), lease.attemptId(), safeReason));
    }

    @Override public boolean authorizePublicationCycle(String attemptId) {
        Optional<PaymentPorts.AttemptSnapshot> before = find(attemptId);
        boolean authorized = Long.valueOf(1).equals(run(AUTHORIZE_PUBLICATION,
            List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId));
        if (authorized) log.info("Checkout v2 cycle authorized attemptId={} cartId={} reasonCode=PUBLICATION_CYCLE_AUTHORIZED",
            attemptId, before.map(PaymentPorts.AttemptSnapshot::cartId).orElse("UNKNOWN"));
        return authorized;
    }

    @Override public boolean authorizeProcessingCycle(String attemptId) {
        Optional<PaymentPorts.AttemptSnapshot> before = find(attemptId);
        boolean authorized = Long.valueOf(1).equals(run(AUTHORIZE_PROCESSING,
            List.of(attemptKey(attemptId), PUBLICATION_DUE), attemptId));
        if (authorized) log.info("Checkout v2 cycle authorized attemptId={} cartId={} reasonCode=PROCESSING_CYCLE_AUTHORIZED",
            attemptId, before.map(PaymentPorts.AttemptSnapshot::cartId).orElse("UNKNOWN"));
        return authorized;
    }

    @Override public PaymentPorts.LeaseInfo inspectLease(String attemptId) {
        Map<Object,Object> values = redis.opsForHash().entries(attemptKey(attemptId));
        if (values.isEmpty()) return new PaymentPorts.LeaseInfo(false, 0);
        String token = optionalString(values, "leaseToken");
        return new PaymentPorts.LeaseInfo(token != null && !token.isBlank(), longValue(values, "leaseUntil"));
    }

    private PaymentPorts.AttemptSnapshot snapshot(Map<Object,Object> values) {
        String attemptId = string(values, "attemptId");
        String cartId = string(values, "cartId");
        PaymentCommand command = mapper.readValue(string(values, "command"), PaymentCommand.class);
        PaymentResult result = null;
        String rawResult = optionalString(values, "result");
        if (rawResult != null) result = mapper.readValue(rawResult, PaymentResult.class);
        return new PaymentPorts.AttemptSnapshot(attemptId, cartId, command, string(values, "state"),
            integer(values, "publicationCount"), integer(values, "executionCount"),
            longValue(values, "nextPublicationAt"), longValue(values, "nextExecutionAt"),
            optionalString(values, "safeReason"), result);
    }

    private static String string(Map<Object,Object> values, String key) {
        String value = optionalString(values, key); if (value == null) throw new IllegalStateException("Invalid v2 attempt state"); return value;
    }
    private static String optionalString(Map<Object,Object> values, String key) { Object value=values.get(key); return value instanceof String s ? s : null; }
    private static int integer(Map<Object,Object> values, String key) { return Math.toIntExact(longValue(values, key)); }
    private static long longValue(Map<Object,Object> values, String key) { String value=optionalString(values,key); return value == null ? 0 : Long.parseLong(value); }
    private static String cartKey(String cartId) { return CART_PREFIX + cartId; }
    private static String attemptKey(String attemptId) { return ATTEMPT_PREFIX + attemptId; }
    private Long run(String script, List<String> keys, String... args) {
        return redis.execute(new DefaultRedisScript<>(script, Long.class), keys, (Object[]) args);
    }
}
