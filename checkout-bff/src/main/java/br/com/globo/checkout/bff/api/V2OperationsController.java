package br.com.globo.checkout.bff.api;

import br.com.globo.checkout.bff.api.Requests.CheckoutRequest;
import br.com.globo.checkout.bff.api.Responses.CheckoutResponse;
import br.com.globo.checkout.bff.application.CheckoutV2Service;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v2/operations")
public class V2OperationsController {
    private final CheckoutV2Service service;
    public V2OperationsController(CheckoutV2Service service) { this.service = service; }

    @PostMapping("/checkout")
    @Operation(summary = "Checkout v2 com Kafka e resposta compatível com v1",
        description = "POST síncrono; não retorna 202, não adiciona cabeçalho de idempotência nem rota GET de consulta. "
            + "Aguarda no máximo cinco segundos no total, incluindo publicação, com até dois segundos para envio. "
            + "HTTP 424 de resultado incerto não cancela o processamento; o pagamento ainda pode ser aprovado.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Pagamento aprovado",
            content = @Content(schema = @Schema(implementation = Responses.CheckoutResponse.class))),
        @ApiResponse(responseCode = "400", description = "Requisição inválida ou meio não permitido",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "404", description = "Carrinho inexistente",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "409", description = "Carrinho aprovado ou tentativa v2 em andamento (P-1005)",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "422", description = "Pagamento recusado",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "424", description = "Dependência indisponível ou resultado incerto; pagamento pode ter ocorrido",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class)))
    })
    public CheckoutResponse checkout(@Valid @RequestBody CheckoutRequest request, HttpServletRequest httpRequest) {
        Object rawDeadline = httpRequest.getAttribute(br.com.globo.checkout.bff.infrastructure.CheckoutV2DeadlineFilter.DEADLINE_ATTRIBUTE);
        long deadline = rawDeadline instanceof Long value ? value : System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        return service.checkout(request, deadline);
    }
}
