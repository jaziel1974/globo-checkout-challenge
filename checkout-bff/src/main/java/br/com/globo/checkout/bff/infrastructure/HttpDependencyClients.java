package br.com.globo.checkout.bff.infrastructure;

import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.CheckoutError.Kind;
import br.com.globo.checkout.bff.domain.Models.PaymentMethod;
import br.com.globo.checkout.bff.domain.Models.Product;
import br.com.globo.checkout.bff.domain.Ports;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** HTTP clients for the catalog services, with an absolute deadline per lookup. */
@Component
public class HttpDependencyClients implements Ports.ProductCatalog, Ports.PaymentMethodCatalog, AutoCloseable {
    private static final long LOOKUP_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(2);
    private static final int MAX_CONCURRENT_LOOKUPS = 32;

    private final CloseableHttpClient client;
    private final String productsUrl;
    private final String methodsUrl;
    private final JsonMapper jsonMapper;
    private final ThreadPoolExecutor lookups;

    public HttpDependencyClients(@Value("${services.products-base-url}") String productsUrl,
                                 @Value("${services.payment-methods-base-url}") String methodsUrl) {
        var connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
            .setDefaultConnectionConfig(ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(2))
                .build())
            .build();
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(Timeout.ofSeconds(2))
            .setResponseTimeout(Timeout.ofSeconds(2))
            .build();
        this.client = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setDefaultRequestConfig(requestConfig)
            .disableAutomaticRetries()
            .build();
        this.productsUrl = stripTrailingSlash(productsUrl);
        this.methodsUrl = stripTrailingSlash(methodsUrl);
        this.jsonMapper = JsonMapper.builder().build();
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, "dependency-http-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.lookups = new ThreadPoolExecutor(
            MAX_CONCURRENT_LOOKUPS, MAX_CONCURRENT_LOOKUPS, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(MAX_CONCURRENT_LOOKUPS), threadFactory,
            new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public Product getProduct(String slug) {
        String body;
        try {
            body = get(productsUrl + "/products/" + encodePathSegment(slug), true);
        } catch (MissingProductException ex) {
            throw new CheckoutError(Kind.PRODUCT_NOT_FOUND, null);
        } catch (RuntimeException ex) {
            throw dependencyUnavailable(ex);
        }
        try {
            Product product = jsonMapper.readValue(body, Product.class);
            if (product == null || blank(product.slug()) || blank(product.name()) || blank(product.description())
                || product.price() == null || product.price().signum() <= 0 || !"BRL".equals(product.currency())) {
                throw new IllegalStateException("Invalid product payload");
            }
            return product;
        } catch (RuntimeException ex) {
            throw dependencyUnavailable(ex);
        }
    }

    @Override
    public List<PaymentMethod> getPaymentMethods() {
        try {
            MethodResponse result = jsonMapper.readValue(get(methodsUrl + "/payment-methods", false), MethodResponse.class);
            if (result == null || result.paymentMethods() == null || result.paymentMethods().isEmpty()
                || result.paymentMethods().stream().anyMatch(method -> method == null || blank(method.name()))) {
                throw new IllegalStateException("Invalid payment methods payload");
            }
            return List.copyOf(result.paymentMethods());
        } catch (RuntimeException ex) {
            throw dependencyUnavailable(ex);
        }
    }

    private String get(String uri, boolean productLookup) {
        HttpGet request = new HttpGet(uri);
        long started = System.nanoTime();
        FutureTask<String> task = new FutureTask<>(() -> client.execute(request, responseHandler(productLookup)));
        try {
            lookups.execute(task);
            long remaining = LOOKUP_DEADLINE_NANOS - (System.nanoTime() - started);
            if (remaining <= 0) throw new TimeoutException("Dependency lookup exceeded two seconds");
            return task.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException ex) {
            request.cancel();
            task.cancel(true);
            throw new DependencyTimeoutException(ex);
        } catch (InterruptedException ex) {
            request.cancel();
            task.cancel(true);
            Thread.currentThread().interrupt();
            throw new DependencyLookupException(ex);
        } catch (ExecutionException | java.util.concurrent.RejectedExecutionException ex) {
            request.cancel();
            Throwable cause = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof MissingProductException missing) throw missing;
            throw new DependencyLookupException(cause);
        }
    }

    private HttpClientResponseHandler<String> responseHandler(boolean productLookup) {
        return (ClassicHttpResponse response) -> {
            int status = response.getCode();
            if (productLookup && status == 404) throw new MissingProductException();
            if (status < 200 || status >= 300) throw new IOException("Dependency returned HTTP " + status);
            HttpEntity entity = response.getEntity();
            if (entity == null) throw new IOException("Dependency returned an empty response");
            return EntityUtils.toString(entity, StandardCharsets.UTF_8);
        };
    }

    private CheckoutError dependencyUnavailable(RuntimeException cause) {
        Throwable root = cause instanceof DependencyLookupException && cause.getCause() != null ? cause.getCause() : cause;
        return new CheckoutError(Kind.DEPENDENCY_UNAVAILABLE, null, root);
    }

    private String encodePathSegment(String value) {
        return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private boolean blank(String value) { return value == null || value.isBlank(); }

    @Override
    public void close() throws IOException {
        lookups.shutdownNow();
        client.close();
    }

    private record MethodResponse(List<PaymentMethod> paymentMethods) {}
    private static final class MissingProductException extends RuntimeException {}
    private static final class DependencyTimeoutException extends RuntimeException {
        private DependencyTimeoutException(Throwable cause) { super(cause); }
    }
    private static final class DependencyLookupException extends RuntimeException {
        private DependencyLookupException(Throwable cause) { super(cause); }
    }
}
