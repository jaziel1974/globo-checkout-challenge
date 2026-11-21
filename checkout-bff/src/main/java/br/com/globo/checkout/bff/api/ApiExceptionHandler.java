package br.com.globo.checkout.bff.api;

import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.CheckoutError.Kind;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@RestControllerAdvice
public class ApiExceptionHandler {
    public record Button(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String label,
                         @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String url) {}
    public record FrontendError(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String code,
                                @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String title,
                                @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String description,
                                @Schema(requiredMode = Schema.RequiredMode.REQUIRED) Button primaryButton,
                                @JsonInclude(NON_NULL) @Schema(nullable = true) Button secondaryButton) {}
    public record ErrorResponse(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String displayType,
                                @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String errorCode,
                                @Schema(requiredMode = Schema.RequiredMode.REQUIRED, nullable = true) String cartId,
                                @Schema(requiredMode = Schema.RequiredMode.REQUIRED) FrontendError frontendError) {}
    private record Definition(HttpStatus status, String display, String errorCode, String code, String title, String description, String label, String url, String secondaryLabel, String secondaryUrl) {}
    private static final Map<Kind, Definition> DEFINITIONS = Map.ofEntries(
        Map.entry(Kind.INVALID_REQUEST, d(HttpStatus.BAD_REQUEST, "INTERVENTION", "INVALID_REQUEST", "V-1001", "Dados inválidos", "Confira os dados informados e tente novamente.", "Tentar novamente", "RETRY")),
        Map.entry(Kind.PAYMENT_METHOD_NOT_ALLOWED, d(HttpStatus.BAD_REQUEST, "INTERVENTION", "PAYMENT_METHOD_NOT_ALLOWED", "P-1002", "Meio de pagamento indisponível", "Escolha um dos meios de pagamento disponíveis para este carrinho.", "Tentar novamente", "RETRY")),
        Map.entry(Kind.PRODUCT_NOT_FOUND, d(HttpStatus.NOT_FOUND, "BLOCK", "PRODUCT_NOT_FOUND", "C-1001", "Produto não encontrado", "Selecione os produtos novamente para iniciar o checkout.", "Escolher produtos", "/checkout/produtos")),
        Map.entry(Kind.CART_NOT_FOUND, d(HttpStatus.NOT_FOUND, "BLOCK", "CART_NOT_FOUND", "C-1002", "Carrinho não encontrado", "Inicie um novo checkout para continuar.", "Escolher produtos", "/checkout/produtos")),
        Map.entry(Kind.PAYMENT_ALREADY_APPROVED, d(HttpStatus.CONFLICT, "BLOCK", "PAYMENT_ALREADY_APPROVED", "P-1003", "Carrinho já pago", "Este carrinho já foi aprovado. Inicie outro checkout para uma nova compra.", "Escolher produtos", "/checkout/produtos")),
        Map.entry(Kind.PAYMENT_DECLINED, new Definition(HttpStatus.UNPROCESSABLE_ENTITY, "BLOCK", "PAYMENT_DECLINED", "P-1001", "Pagamento não aprovado", "Não foi possível concluir o pagamento. Tente novamente ou escolha outro meio.", "Tentar novamente", "RETRY", "Trocar meio de pagamento", "/checkout/pagamento")),
        Map.entry(Kind.DEPENDENCY_UNAVAILABLE, d(HttpStatus.FAILED_DEPENDENCY, "INTERVENTION", "DEPENDENCY_UNAVAILABLE", "S-1001", "Serviço indisponível", "Não foi possível concluir a operação. Tente novamente.", "Tentar novamente", "RETRY")),
        Map.entry(Kind.UNKNOWN_APPROVAL, d(HttpStatus.FAILED_DEPENDENCY, "INTERVENTION", "DEPENDENCY_UNAVAILABLE", "S-1001", "Resultado ainda não confirmado", "O pagamento pode ter sido aprovado. Tente novamente para verificar o estado do carrinho.", "Verificar novamente", "RETRY"))
    );
    private static Definition d(HttpStatus status, String display, String errorCode, String code, String title, String description, String label, String url) { return new Definition(status, display, errorCode, code, title, description, label, url, null, null); }
    @ExceptionHandler(CheckoutError.class)
    public ResponseEntity<ErrorResponse> checkoutError(CheckoutError ex) { return response(ex.kind(), ex.cartId()); }
    @ExceptionHandler({MethodArgumentNotValidException.class, BindException.class})
    public ResponseEntity<ErrorResponse> validationError(Exception ex) {
        Object target = ex instanceof MethodArgumentNotValidException invalid ? invalid.getBindingResult().getTarget() : ((BindException) ex).getBindingResult().getTarget();
        String cartId = target instanceof Requests.CheckoutRequest request ? request.cartId() : null;
        return response(Kind.INVALID_REQUEST, cartId);
    }
    @ExceptionHandler({HttpMessageNotReadableException.class, HttpMediaTypeNotSupportedException.class})
    public ResponseEntity<ErrorResponse> invalidInput(Exception ex) { return response(Kind.INVALID_REQUEST, null); }
    private ResponseEntity<ErrorResponse> response(Kind kind, String cartId) {
        Definition d = DEFINITIONS.get(kind);
        Button primary = new Button(d.label(), d.url());
        Button secondary = d.secondaryLabel() == null ? null : new Button(d.secondaryLabel(), d.secondaryUrl());
        return ResponseEntity.status(d.status()).body(new ErrorResponse(d.display(), d.errorCode(), cartId, new FrontendError(d.code(), d.title(), d.description(), primary, secondary)));
    }
}
