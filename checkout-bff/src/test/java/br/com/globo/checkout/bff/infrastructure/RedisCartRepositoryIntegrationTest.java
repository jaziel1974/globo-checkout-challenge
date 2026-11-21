package br.com.globo.checkout.bff.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import br.com.globo.checkout.bff.domain.Models.Cart;
import br.com.globo.checkout.bff.domain.Models.PaymentMethod;
import br.com.globo.checkout.bff.domain.Models.Product;
import br.com.globo.checkout.bff.domain.Ports.ApprovalResult;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Testcontainers(disabledWithoutDocker = true)
class RedisCartRepositoryIntegrationTest {
    @Container static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    static LettuceConnectionFactory connectionFactory;
    static StringRedisTemplate template;
    @BeforeAll static void startRedis() {
        connectionFactory = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379)); connectionFactory.afterPropertiesSet();
        template = new StringRedisTemplate(connectionFactory); template.afterPropertiesSet();
    }
    @AfterAll static void stopRedis() { if (connectionFactory != null) connectionFactory.destroy(); redis.stop(); }
    @Test void atomicApprovalAllowsOnlyOneOfConcurrentRepositories() throws Exception {
        var first = new RedisCartRepository(template); var second = new RedisCartRepository(template);
        String id = java.util.UUID.randomUUID().toString();
        first.create(new Cart(id, "PENDENTE", List.of(new Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "BRL")), List.of(new PaymentMethod("Pix"))));
        assertEquals("PENDENTE", second.find(id).orElseThrow().status());
        var pool = Executors.newFixedThreadPool(16);
        try {
            List<java.util.concurrent.Future<ApprovalResult>> approvals = java.util.stream.IntStream.range(0, 32)
                .mapToObj(i -> pool.submit(() -> (i % 2 == 0 ? first : second).approve(id))).toList();
            long winners = approvals.stream().filter(f -> { try { return f.get(10, TimeUnit.SECONDS) == ApprovalResult.APPROVED; } catch (Exception e) { throw new RuntimeException(e); } }).count();
            assertEquals(1, winners); assertEquals(ApprovalResult.ALREADY_APPROVED, first.approve(id)); assertEquals("APROVADO", first.find(id).orElseThrow().status());
        } finally { pool.shutdownNow(); }
    }
    @Test void approvalForMissingCartDoesNotCreateState() { assertEquals(ApprovalResult.NOT_FOUND, new RedisCartRepository(template).approve("missing-" + java.util.UUID.randomUUID())); }
}
