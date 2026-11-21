package br.com.globo.checkout.bff.api;

import static org.junit.jupiter.api.Assertions.*;
import br.com.globo.checkout.bff.domain.CheckoutError;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.ObjectMapper;

class ApiContractTest {
    private final ApiExceptionHandler handler = new ApiExceptionHandler();
    private final ObjectMapper mapper = new ObjectMapper();
    @Test void businessErrorsKeepRequiredFieldsAndNullCartId() throws Exception {
        var response = handler.checkoutError(new CheckoutError(CheckoutError.Kind.PRODUCT_NOT_FOUND, null));
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode()); assertNull(response.getBody().cartId());
        var json = mapper.writeValueAsString(response.getBody());
        assertTrue(json.contains("\"cartId\":null")); assertTrue(json.contains("\"displayType\":\"BLOCK\"")); assertTrue(json.contains("\"errorCode\":\"PRODUCT_NOT_FOUND\""));
        assertTrue(json.contains("\"primaryButton\"")); assertFalse(json.contains("secondaryButton"));
    }
    @Test void paymentResponseOmitsTheOtherMethodCredentialField() throws Exception {
        var pix = new Responses.PaymentView("Pix", null, "PIX-SIMULADO-id", new BigDecimal("39.90"), "BRL");
        var json = mapper.writeValueAsString(pix);
        assertTrue(json.contains("\"pixCode\"")); assertFalse(json.contains("lastDigits"));
        var declined = handler.checkoutError(new CheckoutError(CheckoutError.Kind.PAYMENT_DECLINED, "cart-1")).getBody();
        assertEquals("P-1001", declined.frontendError().code()); assertEquals("/checkout/pagamento", declined.frontendError().secondaryButton().url());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, handler.checkoutError(new CheckoutError(CheckoutError.Kind.PAYMENT_DECLINED, "cart-1")).getStatusCode());
    }
}
