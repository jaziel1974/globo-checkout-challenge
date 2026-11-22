package br.com.globo.checkout.worker.application;

import br.com.globo.checkout.worker.domain.PaymentBusinessException;
import br.com.globo.checkout.worker.domain.PaymentCommand;
import br.com.globo.checkout.worker.domain.PaymentResult;
import br.com.globo.checkout.worker.domain.PaymentSimulator;
import org.springframework.stereotype.Service;

/** Coordinates v2 business calculation and atomic result persistence without depending on Kafka. */
@Service
public class PaymentProcessor {
    private final PaymentPorts.AttemptStore attempts;
    private final PaymentPorts.CartSnapshotReader carts;
    private final PaymentSimulator simulator;

    public PaymentProcessor(PaymentPorts.AttemptStore attempts, PaymentPorts.CartSnapshotReader carts, PaymentSimulator simulator) {
        this.attempts = attempts;
        this.carts = carts;
        this.simulator = simulator;
    }

    public PaymentResult process(PaymentCommand command, PaymentPorts.ProcessingLease lease) {
        var attempt = attempts.find(command.attemptId()).orElseThrow(() -> new IllegalStateException("Attempt state missing"));
        if (!attempt.attemptId().equals(lease.attemptId()) || !attempt.command().equals(command)
            || !"PROCESSANDO".equals(attempt.state())) throw new IllegalStateException("Command does not match active attempt");
        var execution = attempts.reserveExecution(lease);
        if (execution.isEmpty() || execution.get() == 0) throw new IllegalStateException("Execution was not reserved");
        var cart = carts.find(command.cartId()).orElseThrow(() -> new PaymentBusinessException(PaymentBusinessException.Kind.CART_NOT_FOUND));
        PaymentResult result = simulator.simulate(command, cart);
        PaymentPorts.CompletionResult completion = attempts.complete(lease, result);
        if (completion != PaymentPorts.CompletionResult.COMPLETED) throw new IllegalStateException("Attempt ownership lost before completion");
        return result;
    }
}
