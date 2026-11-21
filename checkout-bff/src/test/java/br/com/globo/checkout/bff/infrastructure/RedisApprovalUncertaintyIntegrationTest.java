package br.com.globo.checkout.bff.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import br.com.globo.checkout.bff.domain.Models.Cart;
import br.com.globo.checkout.bff.domain.Models.PaymentMethod;
import br.com.globo.checkout.bff.domain.Models.Product;
import br.com.globo.checkout.bff.domain.Ports.ApprovalResult;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;
import io.lettuce.core.ClientOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class RedisApprovalUncertaintyIntegrationTest {
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    private TcpProxy proxy;
    private LettuceConnectionFactory connectionFactory;
    private RedisCartRepository repository;

    @BeforeEach void setUp() throws Exception {
        proxy = new TcpProxy(REDIS.getHost(), REDIS.getMappedPort(6379));
        repository = repository("127.0.0.1", proxy.port(), false);
    }
    @AfterEach void tearDown() {
        if (connectionFactory != null) connectionFactory.destroy();
        if (proxy != null) proxy.close();
    }

    @Test @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void droppedReplyAfterAppliedScriptIsDiscoveredWithoutSecondApproval() {
        String id = createCart();
        proxy.dropNextReply();
        assertThrows(RuntimeException.class, () -> repository.approve(id));
        RedisCartRepository verifier = directRepository();
        assertEquals("APROVADO", verifier.find(id).orElseThrow().status());
        assertEquals(ApprovalResult.ALREADY_APPROVED, verifier.approve(id));
    }

    @Test @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void droppedCommandLeavesCartPendingAndAllowsRetry() {
        String id = createCart();
        proxy.dropNextCommand();
        assertThrows(RuntimeException.class, () -> repository.approve(id));
        RedisCartRepository verifier = directRepository();
        assertEquals("PENDENTE", verifier.find(id).orElseThrow().status());
        assertEquals(ApprovalResult.APPROVED, verifier.approve(id));
    }

    private String createCart() {
        String id = UUID.randomUUID().toString();
        repository.create(new Cart(id, "PENDENTE", List.of(new Product("premium", "Plano Premium", "Assinatura", new java.math.BigDecimal("39.90"), "BRL")), List.of(new PaymentMethod("Pix"))));
        assertTrue(repository.find(id).isPresent()); return id;
    }

    private RedisCartRepository directRepository() {
        if (connectionFactory != null) connectionFactory.destroy();
        return repository(REDIS.getHost(), REDIS.getMappedPort(6379), true);
    }

    private RedisCartRepository repository(String host, int port, boolean autoReconnect) {
        RedisStandaloneConfiguration server = new RedisStandaloneConfiguration(host, port);
        LettuceClientConfiguration client = LettuceClientConfiguration.builder()
            .clientOptions(ClientOptions.builder().autoReconnect(autoReconnect).build())
            .commandTimeout(Duration.ofSeconds(1))
            .shutdownTimeout(Duration.ZERO)
            .build();
        connectionFactory = new LettuceConnectionFactory(server, client);
        connectionFactory.afterPropertiesSet();
        StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
        template.afterPropertiesSet();
        return new RedisCartRepository(template);
    }

    /** Forwards Redis RESP traffic and cuts exactly the next command or reply when asked. */
    private static final class TcpProxy implements AutoCloseable {
        private enum Cut { NONE, COMMAND, REPLY }
        private final ServerSocket listener;
        private final String upstreamHost;
        private final int upstreamPort;
        private final AtomicReference<Cut> nextCut = new AtomicReference<>(Cut.NONE);
        private final AtomicBoolean cutApprovalReply = new AtomicBoolean();
        private final ExecutorService workers = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r, "redis-test-proxy"); t.setDaemon(true); return t; });
        private final Thread acceptor;
        private volatile boolean closed;
        TcpProxy(String upstreamHost, int upstreamPort) throws IOException {
            this.upstreamHost = upstreamHost; this.upstreamPort = upstreamPort;
            listener = new ServerSocket(0, 50, java.net.InetAddress.getByName("127.0.0.1"));
            acceptor = new Thread(this::acceptLoop, "redis-test-proxy-accept"); acceptor.setDaemon(true); acceptor.start();
        }
        int port() { return listener.getLocalPort(); }
        void dropNextCommand() { nextCut.set(Cut.COMMAND); }
        void dropNextReply() { nextCut.set(Cut.REPLY); }
        private void acceptLoop() {
            while (!closed) try {
                Socket client = listener.accept(); workers.execute(() -> handle(client));
            } catch (IOException e) { if (!closed) throw new IllegalStateException(e); }
        }
        private void handle(Socket client) {
            try {
                Socket upstream = new Socket(upstreamHost, upstreamPort);
                workers.execute(() -> copy(client, upstream, Cut.COMMAND));
                workers.execute(() -> copy(upstream, client, Cut.REPLY));
            } catch (IOException e) { closeQuietly(client); }
        }
        private void copy(Socket source, Socket destination, Cut direction) {
            try {
                byte[] buffer = new byte[8192]; int count;
                while ((count = source.getInputStream().read(buffer)) >= 0) {
                    if (direction == Cut.COMMAND && containsApprovalScript(buffer, count)) {
                        if (nextCut.compareAndSet(Cut.COMMAND, Cut.NONE)) {
                            closeQuietly(source); closeQuietly(destination); return;
                        }
                        if (containsEval(buffer, count) && nextCut.compareAndSet(Cut.REPLY, Cut.NONE)) cutApprovalReply.set(true);
                    } else if (direction == Cut.REPLY && cutApprovalReply.compareAndSet(true, false)) {
                        closeQuietly(source); closeQuietly(destination); return;
                    }
                    destination.getOutputStream().write(buffer, 0, count); destination.getOutputStream().flush();
                }
            } catch (IOException ignored) { } finally { closeQuietly(source); closeQuietly(destination); }
        }
        private static boolean containsApprovalScript(byte[] bytes, int length) {
            String command = new String(bytes, 0, length, java.nio.charset.StandardCharsets.US_ASCII);
            return command.contains("$4\r\nEVAL\r\n") || command.contains("$7\r\nEVALSHA\r\n");
        }
        private static boolean containsEval(byte[] bytes, int length) {
            return new String(bytes, 0, length, java.nio.charset.StandardCharsets.US_ASCII).contains("$4\r\nEVAL\r\n");
        }
        private static void closeQuietly(Socket socket) { try { socket.close(); } catch (IOException ignored) { } }
        @Override public void close() {
            closed = true; try { listener.close(); } catch (IOException ignored) { }
            workers.shutdownNow();
        }
    }
}
