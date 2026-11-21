package br.com.globo.checkout.bff.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import br.com.globo.checkout.bff.api.Responses.CheckoutProductView;
import br.com.globo.checkout.bff.api.Responses.CheckoutResponse;
import br.com.globo.checkout.bff.api.Responses.PaymentView;
import br.com.globo.checkout.bff.api.Responses.PreCheckoutResponse;
import br.com.globo.checkout.bff.api.Responses.ProductView;
import br.com.globo.checkout.bff.api.Responses.PaymentMethodView;
import br.com.globo.checkout.bff.application.CheckoutService;
import br.com.globo.checkout.bff.domain.CheckoutError;
import br.com.globo.checkout.bff.domain.Models;
import br.com.globo.checkout.bff.domain.Ports;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class OperationsHttpContractTest {
    private CheckoutService service;
    private MockMvc mvc;

    @BeforeEach void setUp() {
        service = mock(CheckoutService.class);
        mvc = MockMvcBuilders.standaloneSetup(new OperationsController(service))
            .setControllerAdvice(new ApiExceptionHandler()).build();
    }

    @Test void preCheckoutSuccessHasCompleteFrontendContract() throws Exception {
        when(service.preCheckout(any())).thenReturn(new PreCheckoutResponse("cart-1",
            List.of(new ProductView("premium", "Plano Premium", "Assinatura mensal", new BigDecimal("39.90"), "BRL")),
            List.of(new PaymentMethodView("Cartão de Crédito"), new PaymentMethodView("Pix"))));

        mvc.perform(post("/operations/pre-checkout").contentType(MediaType.APPLICATION_JSON)
                .content("{\"products\":[{\"slug\":\"premium\"}]}") )
            .andExpect(status().isOk()).andExpect(jsonPath("$.cartId").value("cart-1"))
            .andExpect(jsonPath("$.products[0].slug").value("premium"))
            .andExpect(jsonPath("$.products[0].name").value("Plano Premium"))
            .andExpect(jsonPath("$.products[0].description").value("Assinatura mensal"))
            .andExpect(jsonPath("$.products[0].price").value(39.90))
            .andExpect(jsonPath("$.products[0].currency").value("BRL"))
            .andExpect(jsonPath("$.paymentMethods[0].name").value("Cartão de Crédito"))
            .andExpect(jsonPath("$.paymentMethods[1].name").value("Pix"));
    }

    @Test void checkoutCardSuccessHasCompleteContractAndOnlyLastDigits() throws Exception {
        when(service.checkout(any())).thenReturn(new CheckoutResponse("cart-1", "APROVADO", Instant.parse("2026-10-14T16:00:00Z"),
            new PaymentView("Cartão de Crédito", "4242", null, new BigDecimal("39.90"), "BRL"),
            List.of(new CheckoutProductView("premium", "Plano Premium", new BigDecimal("39.90")))));

        mvc.perform(post("/operations/checkout").contentType(MediaType.APPLICATION_JSON).content(cardRequest("4111111111114242")))
            .andExpect(status().isOk()).andExpect(jsonPath("$.cartId").value("cart-1"))
            .andExpect(jsonPath("$.status").value("APROVADO"))
            .andExpect(jsonPath("$.paidAt").value("2026-10-14T16:00:00Z"))
            .andExpect(jsonPath("$.payment.method").value("Cartão de Crédito"))
            .andExpect(jsonPath("$.payment.lastDigits").value("4242"))
            .andExpect(jsonPath("$.payment.amount").value(39.90))
            .andExpect(jsonPath("$.payment.currency").value("BRL"))
            .andExpect(jsonPath("$.payment.pixCode").doesNotExist())
            .andExpect(jsonPath("$.products[0].slug").value("premium"))
            .andExpect(jsonPath("$.products[0].name").value("Plano Premium"))
            .andExpect(jsonPath("$.products[0].price").value(39.90));
    }

    @Test void checkoutPixSuccessHasPixCodeAndNoCardDigits() throws Exception {
        when(service.checkout(any())).thenReturn(new CheckoutResponse("cart-1", "APROVADO", Instant.parse("2026-10-14T16:00:00Z"),
            new PaymentView("Pix", null, "PIX-SIMULADO-cart-1", new BigDecimal("39.90"), "BRL"),
            List.of(new CheckoutProductView("premium", "Plano Premium", new BigDecimal("39.90")))));
        mvc.perform(post("/operations/checkout").contentType(MediaType.APPLICATION_JSON)
                .content("{\"cartId\":\"cart-1\",\"paymentMethod\":{\"name\":\"Pix\"}}"))
            .andExpect(status().isOk()).andExpect(jsonPath("$.payment.pixCode").value("PIX-SIMULADO-cart-1"))
            .andExpect(jsonPath("$.payment.lastDigits").doesNotExist());
    }

    @Test void allBusinessFailuresUseTheStableFrontendErrorEnvelope() throws Exception {
        for (CheckoutError.Kind kind : CheckoutError.Kind.values()) {
            reset(service);
            when(service.checkout(any())).thenThrow(new CheckoutError(kind, "cart-1"));
            mvc.perform(post("/operations/checkout").contentType(MediaType.APPLICATION_JSON)
                    .content("{\"cartId\":\"cart-1\",\"paymentMethod\":{\"name\":\"Pix\"}}"))
                .andExpect(status().is(kind == CheckoutError.Kind.INVALID_REQUEST || kind == CheckoutError.Kind.PAYMENT_METHOD_NOT_ALLOWED ? 400
                    : kind == CheckoutError.Kind.PRODUCT_NOT_FOUND || kind == CheckoutError.Kind.CART_NOT_FOUND ? 404
                    : kind == CheckoutError.Kind.PAYMENT_ALREADY_APPROVED ? 409
                    : kind == CheckoutError.Kind.PAYMENT_DECLINED ? 422 : 424))
                .andExpect(jsonPath("$.displayType").isNotEmpty()).andExpect(jsonPath("$.errorCode").value(kind == CheckoutError.Kind.UNKNOWN_APPROVAL ? "DEPENDENCY_UNAVAILABLE" : kind.name()))
                .andExpect(jsonPath("$.cartId").value("cart-1"))
                .andExpect(jsonPath("$.frontendError.code").isNotEmpty())
                .andExpect(jsonPath("$.frontendError.title").isNotEmpty())
                .andExpect(jsonPath("$.frontendError.description").isNotEmpty())
                .andExpect(jsonPath("$.frontendError.primaryButton.label").isNotEmpty())
                .andExpect(jsonPath("$.frontendError.primaryButton.url").isNotEmpty());
        }
    }

    @Test void validationFailurePreservesKnownCartId() throws Exception {
        mvc.perform(post("/operations/checkout").contentType(MediaType.APPLICATION_JSON)
                .content("{\"cartId\":\"cart-known\",\"paymentMethod\":{\"name\":\"Cartão de Crédito\",\"card\":{\"holderName\":\"Ana\",\"number\":\"4111\",\"expiry\":\"\",\"cvv\":\"123\"}}}") )
            .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"))
            .andExpect(jsonPath("$.cartId").value("cart-known"))
            .andExpect(jsonPath("$.frontendError.code").value("V-1001"));
    }

    @Test void approvedCartWithDeclineTestCardReturnsAlreadyApprovedOverHttp() throws Exception {
        Ports.CartRepository carts = mock(Ports.CartRepository.class);
        when(carts.find("cart-paid")).thenReturn(Optional.of(new Models.Cart("cart-paid", "APROVADO",
            List.of(new Models.Product("premium", "Plano Premium", "Assinatura", new BigDecimal("39.90"), "BRL")),
            List.of(new Models.PaymentMethod("Cartão de Crédito")))));
        CheckoutService realService = new CheckoutService(mock(Ports.ProductCatalog.class), mock(Ports.PaymentMethodCatalog.class), carts);
        mvc = MockMvcBuilders.standaloneSetup(new OperationsController(realService))
            .setControllerAdvice(new ApiExceptionHandler()).build();

        mvc.perform(post("/operations/checkout").contentType(MediaType.APPLICATION_JSON)
                .content(cardRequest("4111111111110002").replace("cart-1", "cart-paid")))
            .andExpect(status().isConflict()).andExpect(jsonPath("$.errorCode").value("PAYMENT_ALREADY_APPROVED"))
            .andExpect(jsonPath("$.cartId").value("cart-paid"))
            .andExpect(jsonPath("$.frontendError.code").value("P-1003"));
    }

    @ParameterizedTest @MethodSource("nonStringCardFields")
    void rejectsJsonValuesThatAreNotStringsForEveryCardField(String field, String value) throws Exception {
        String body = "{\"cartId\":\"cart-1\",\"paymentMethod\":{\"name\":\"Cartão de Crédito\",\"card\":{\"holderName\":\"Ana\",\"number\":\"4111111111114242\",\"expiry\":\"12/28\",\"cvv\":\"123\"}}}";
        String invalid = body.replace("\"" + field + "\":\"" + (field.equals("holderName") ? "Ana" : field.equals("number") ? "4111111111114242" : field.equals("expiry") ? "12/28" : "123") + "\"", "\"" + field + "\":" + value);
        mvc.perform(post("/operations/checkout").contentType(MediaType.APPLICATION_JSON).content(invalid))
            .andExpect(status().isBadRequest()).andExpect(jsonPath("$.displayType").value("INTERVENTION"))
            .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"))
            .andExpect(jsonPath("$.frontendError.code").value("V-1001"));
    }

    @Test void unsupportedMediaTypeUsesFrontendErrorContract() throws Exception {
        mvc.perform(post("/operations/checkout").contentType(MediaType.TEXT_PLAIN).content("not-json"))
            .andExpect(status().isBadRequest()).andExpect(jsonPath("$.displayType").value("INTERVENTION"))
            .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"))
            .andExpect(jsonPath("$.cartId").value(org.hamcrest.Matchers.nullValue()))
            .andExpect(jsonPath("$.frontendError.code").value("V-1001"))
            .andExpect(jsonPath("$.frontendError.primaryButton.label").value("Tentar novamente"));
    }

    private static Stream<org.junit.jupiter.params.provider.Arguments> nonStringCardFields() {
        return Stream.of(org.junit.jupiter.params.provider.Arguments.of("holderName", "123"),
            org.junit.jupiter.params.provider.Arguments.of("number", "true"),
            org.junit.jupiter.params.provider.Arguments.of("expiry", "{}"),
            org.junit.jupiter.params.provider.Arguments.of("cvv", "[]"));
    }

    private static String cardRequest(String number) {
        return "{\"cartId\":\"cart-1\",\"paymentMethod\":{\"name\":\"Cartão de Crédito\",\"card\":{\"holderName\":\"Ana Souza\",\"number\":\"" + number + "\",\"expiry\":\"12/28\",\"cvv\":\"123\"}}}";
    }
}
