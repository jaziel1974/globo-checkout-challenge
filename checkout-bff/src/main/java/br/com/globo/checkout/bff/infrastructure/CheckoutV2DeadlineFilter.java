package br.com.globo.checkout.bff.infrastructure;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Captures the monotonic request deadline before MVC parses and validates the v2 body. */
@Component
public final class CheckoutV2DeadlineFilter extends OncePerRequestFilter {
    public static final String DEADLINE_ATTRIBUTE = CheckoutV2DeadlineFilter.class.getName() + ".deadlineNanos";
    private static final long BUDGET_NANOS = Duration.ofSeconds(5).toNanos();

    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equalsIgnoreCase(request.getMethod()) || !"/v2/operations/checkout".equals(request.getRequestURI());
    }

    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long start = System.nanoTime();
        long deadline = start + BUDGET_NANOS;
        if (deadline < 0 && start > 0) deadline = Long.MAX_VALUE;
        request.setAttribute(DEADLINE_ATTRIBUTE, deadline);
        chain.doFilter(request, response);
    }
}
