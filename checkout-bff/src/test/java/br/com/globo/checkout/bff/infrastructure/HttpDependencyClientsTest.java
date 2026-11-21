package br.com.globo.checkout.bff.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import br.com.globo.checkout.bff.api.Requests.PreCheckoutRequest;
import br.com.globo.checkout.bff.api.Requests.ProductSelection;
import br.com.globo.checkout.bff.application.CheckoutService;
import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.Ports;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpDependencyClientsTest {
    private static final String PRODUCT = "{\"slug\":\"premium\",\"name\":\"Plano Premium\",\"description\":\"Assinatura mensal\",\"price\":39.90,\"currency\":\"BRL\"}";
    private static final String METHODS = "{\"paymentMethods\":[{\"name\":\"Pix\"}]}";

    private HttpServer server;
    private volatile String productResponse;
    private volatile int productStatus;
    private volatile long productDelay;
    private volatile boolean dropProductConnection;
    private volatile boolean dripProductResponse;
    private volatile String methodsResponse;
    private volatile int methodsStatus;
    private volatile long methodsDelay;
    private volatile boolean dropMethodsConnection;
    private volatile boolean dripMethodsResponse;
    private final AtomicInteger productRequests = new AtomicInteger();
    private final AtomicInteger methodsRequests = new AtomicInteger();
    private ExecutorService serverExecutor;
    private HttpDependencyClients clients;
    private Ports.CartRepository carts;
    private CheckoutService service;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/products/", exchange -> {
            productRequests.incrementAndGet();
            respond(exchange, productStatus, productResponse, productDelay, dropProductConnection, dripProductResponse);
        });
        server.createContext("/payment-methods", exchange -> {
            methodsRequests.incrementAndGet();
            respond(exchange, methodsStatus, methodsResponse, methodsDelay, dropMethodsConnection, dripMethodsResponse);
        });
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.start();

        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        clients = new HttpDependencyClients(base, base);
        carts = mock(Ports.CartRepository.class);
        service = new CheckoutService(clients, clients, carts);
        productStatus = 200;
        productResponse = PRODUCT;
        productDelay = 0;
        dropProductConnection = false;
        dripProductResponse = false;
        methodsStatus = 200;
        methodsResponse = METHODS;
        methodsDelay = 0;
        dropMethodsConnection = false;
        dripMethodsResponse = false;
    }

    @AfterEach
    void stop() throws Exception {
        clients.close();
        server.stop(0);
        serverExecutor.shutdownNow();
    }

    @Test
    void parsesValidServiceResponses() {
        assertEquals("Plano Premium", clients.getProduct("premium").name());
        assertEquals("Pix", clients.getPaymentMethods().getFirst().name());
    }

    @Test
    void distinguishesMissingProductFromDependencyFailure() {
        productStatus = 404;
        assertEquals(CheckoutError.Kind.PRODUCT_NOT_FOUND,
            assertThrows(CheckoutError.class, () -> clients.getProduct("missing")).kind());

        productStatus = 503;
        assertDependencyFailure(() -> clients.getProduct("premium"));
    }

    @Test
    void productClientRejectsInvalidPayloadsAndConnectionLoss() {
        productResponse = "not-json";
        assertDependencyFailure(() -> clients.getProduct("premium"));
        productResponse = "{}";
        assertDependencyFailure(() -> clients.getProduct("premium"));

        productResponse = PRODUCT;
        dropProductConnection = true;
        assertDependencyFailure(() -> clients.getProduct("premium"));
    }

    @Test
    void paymentMethodClientMapsUnavailableConnectionAndInvalidPayloads() {
        methodsStatus = 503;
        assertDependencyFailure(() -> clients.getPaymentMethods());

        methodsStatus = 200;
        dropMethodsConnection = true;
        assertDependencyFailure(() -> clients.getPaymentMethods());

        dropMethodsConnection = false;
        methodsResponse = "not-json";
        assertDependencyFailure(() -> clients.getPaymentMethods());
        methodsResponse = "{}";
        assertDependencyFailure(() -> clients.getPaymentMethods());
        methodsResponse = "{\"paymentMethods\":[{\"name\":\" \"}]}";
        assertDependencyFailure(() -> clients.getPaymentMethods());
    }

    @Test
    void enforcesTwoSecondTimeoutForBothDependencies() {
        productDelay = 2600;
        assertTimeoutFailure(() -> clients.getProduct("premium"));

        productDelay = 0;
        methodsDelay = 2600;
        assertTimeoutFailure(() -> clients.getPaymentMethods());
    }

    @Test
    void doesNotCreateCartWhenProductDependencyFails() {
        productStatus = 503;
        assertPreCheckoutDependencyFailure();
    }

    @Test
    void doesNotCreateCartWhenPaymentMethodDependencyFailsAfterProductLookup() {
        methodsResponse = "not-json";
        assertPreCheckoutDependencyFailure();
    }

    @Test
    void preCheckoutDoesNotRetryWhenProductConnectionIsClosedWithoutResponse() {
        dropProductConnection = true;

        assertPreCheckoutDependencyFailure();

        assertEquals(1, productRequests.get(), "Product lookup should be sent exactly once");
        assertEquals(0, methodsRequests.get(), "Payment methods are not requested after product lookup fails");
    }

    @Test
    void preCheckoutDoesNotRetryWhenPaymentMethodsConnectionIsClosedWithoutResponse() {
        dropMethodsConnection = true;

        assertPreCheckoutDependencyFailure();

        assertEquals(1, productRequests.get(), "Product lookup should be sent exactly once");
        assertEquals(1, methodsRequests.get(), "Payment methods lookup should be sent exactly once");
    }

    @Test
    void doesNotCreateCartWhenEitherDependencyTimesOut() {
        productDelay = 2600;
        assertPreCheckoutDependencyFailure();

        productDelay = 0;
        methodsDelay = 2600;
        assertPreCheckoutDependencyFailure();
    }

    @Test
    void enforcesAbsoluteDeadlineForProgressivelySlowProductResponse() {
        dripProductResponse = true;

        assertFailsWithinDeadlineWithoutCart();

        assertEquals(1, productRequests.get(), "A slow product response should cause one request only");
        assertEquals(0, methodsRequests.get(), "Payment methods must not be requested after product timeout");
    }

    @Test
    void enforcesAbsoluteDeadlineForProgressivelySlowPaymentMethodsResponse() {
        dripMethodsResponse = true;

        assertFailsWithinDeadlineWithoutCart();

        assertEquals(1, productRequests.get(), "The product lookup should happen once before payment methods");
        assertEquals(1, methodsRequests.get(), "A slow payment methods response should cause one request only");
    }

    private void assertPreCheckoutDependencyFailure() {
        CheckoutError error = assertThrows(CheckoutError.class, () -> service.preCheckout(
            new PreCheckoutRequest(List.of(new ProductSelection("premium")))));
        assertEquals(CheckoutError.Kind.DEPENDENCY_UNAVAILABLE, error.kind());
        verify(carts, never()).create(any());
    }

    private void assertFailsWithinDeadlineWithoutCart() {
        long started = System.nanoTime();
        assertPreCheckoutDependencyFailure();
        assertTrue((System.nanoTime() - started) < 2_500_000_000L,
            "A continuously progressing response must still fail by its two-second absolute deadline");
    }

    private void assertDependencyFailure(Runnable call) {
        assertEquals(CheckoutError.Kind.DEPENDENCY_UNAVAILABLE,
            assertThrows(CheckoutError.class, call::run).kind());
    }

    private void assertTimeoutFailure(Runnable call) {
        long started = System.nanoTime();
        assertDependencyFailure(call);
        assertTrue((System.nanoTime() - started) < 4_000_000_000L,
            "Dependency timeout should be bounded near the configured two seconds");
    }

    private void respond(HttpExchange exchange, int status, String response, long delay, boolean dropConnection,
                         boolean dripResponse) throws IOException {
        try {
            if (delay > 0) Thread.sleep(delay);
            if (dropConnection) {
                exchange.close();
                return;
            }
            byte[] body = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            if (dripResponse) {
                exchange.sendResponseHeaders(status, 0);
                OutputStream output = exchange.getResponseBody();
                int chunkSize = 4;
                for (int offset = 0; offset < body.length; offset += chunkSize) {
                    int length = Math.min(chunkSize, body.length - offset);
                    output.write(body, offset, length);
                    output.flush();
                    Thread.sleep(350);
                }
            } else {
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
        }
    }
}
