package br.com.globo.checkout.bff.api;

import br.com.globo.checkout.bff.application.CheckoutService;
import br.com.globo.checkout.bff.api.Requests.CheckoutRequest;
import br.com.globo.checkout.bff.api.Requests.PreCheckoutRequest;
import br.com.globo.checkout.bff.api.Responses.CheckoutResponse;
import br.com.globo.checkout.bff.api.Responses.PreCheckoutResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operations")
@Tag(name = "Operacoes de checkout")
public class OperationsController {
    private final CheckoutService service;
    public OperationsController(CheckoutService service) { this.service = service; }
    @PostMapping("/pre-checkout")
    @Operation(summary = "Criar carrinho de pre-checkout")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Carrinho criado",
            content = @Content(schema = @Schema(implementation = PreCheckoutResponse.class))),
        @ApiResponse(responseCode = "400", description = "Entrada invalida",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "404", description = "Produto inexistente",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "424", description = "Dependencia indisponivel",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class)))
    })
    public PreCheckoutResponse preCheckout(@Valid @RequestBody PreCheckoutRequest request) { return service.preCheckout(request); }
    @PostMapping("/checkout")
    @Operation(summary = "Simular cobranca do carrinho")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Pagamento aprovado",
            content = @Content(schema = @Schema(implementation = CheckoutResponse.class))),
        @ApiResponse(responseCode = "400", description = "Entrada invalida ou meio nao permitido",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "404", description = "Carrinho inexistente",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "409", description = "Carrinho ja aprovado",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "422", description = "Pagamento recusado",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class))),
        @ApiResponse(responseCode = "424", description = "Dependencia indisponivel ou resultado incerto",
            content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ErrorResponse.class)))
    })
    public CheckoutResponse checkout(@Valid @RequestBody CheckoutRequest request) { return service.checkout(request); }
}
