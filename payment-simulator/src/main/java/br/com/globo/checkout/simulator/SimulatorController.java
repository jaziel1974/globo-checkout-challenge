package br.com.globo.checkout.simulator;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "Catalogo e meios de pagamento")
public class SimulatorController {
    private static final Map<String, Product> PRODUCTS = Map.of(
        "basico", new Product("basico", "Plano Básico", "Assinatura mensal do plano Básico", new BigDecimal("19.90"), "BRL"),
        "premium", new Product("premium", "Plano Premium", "Assinatura mensal com catálogo completo", new BigDecimal("39.90"), "BRL"),
        "familia", new Product("familia", "Plano Família", "Assinatura mensal do plano Família", new BigDecimal("59.90"), "BRL")
    );
    private static final List<PaymentMethod> PAYMENT_METHODS = List.of(
        new PaymentMethod("Cartão de Crédito"), new PaymentMethod("Cartão de Débito"), new PaymentMethod("Pix")
    );

    @GetMapping("/products/{slug}")
    @Operation(summary = "Consultar produto por slug")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Produto encontrado",
            content = @Content(schema = @Schema(implementation = Product.class))),
        @ApiResponse(responseCode = "404", description = "Produto inexistente")
    })
    public ResponseEntity<Product> product(@PathVariable String slug) {
        Product product = PRODUCTS.get(slug);
        if (product == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(product);
    }

    @GetMapping("/payment-methods")
    @Operation(summary = "Listar meios de pagamento")
    @ApiResponse(responseCode = "200", description = "Meios disponiveis")
    public Map<String, List<PaymentMethod>> paymentMethods() { return Map.of("paymentMethods", PAYMENT_METHODS); }

    public record Product(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String slug,
                          @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String name,
                          @Schema(requiredMode = Schema.RequiredMode.REQUIRED) String description,
                          @Schema(requiredMode = Schema.RequiredMode.REQUIRED, type = "number", example = "39.90") BigDecimal price,
                          @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "BRL") String currency) {}
    public record PaymentMethod(@Schema(requiredMode = Schema.RequiredMode.REQUIRED) String name) {}
}
