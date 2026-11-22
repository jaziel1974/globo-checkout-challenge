package br.com.globo.checkout.bff.api;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** The new error is scoped to the v2 controller, leaving the v1 advice unchanged. */
@RestControllerAdvice(assignableTypes = V2OperationsController.class)
@Order(Ordered.HIGHEST_PRECEDENCE)
public class V2PaymentInProgressHandler {
    @ExceptionHandler(PaymentInProgressException.class)
    public ResponseEntity<ApiExceptionHandler.ErrorResponse> paymentInProgress(PaymentInProgressException ex) {
        var primary = new ApiExceptionHandler.Button("Tentar novamente", "RETRY");
        var frontend = new ApiExceptionHandler.FrontendError("P-1005", "Pagamento em processamento",
            "Já existe um pagamento em andamento para este carrinho. Tente novamente mais tarde.", primary, null);
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new ApiExceptionHandler.ErrorResponse("INTERVENTION", "PAYMENT_IN_PROGRESS", ex.cartId(), frontend));
    }
}
