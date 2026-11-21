package br.com.globo.checkout.simulator;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class SimulatorHttpContractTest {
    private MockMvc mvc;
    @BeforeEach void setUp() { mvc = MockMvcBuilders.standaloneSetup(new SimulatorController()).build(); }

    @Test void existingProductReturnsProductContract() throws Exception {
        mvc.perform(get("/products/premium")).andExpect(status().isOk())
            .andExpect(jsonPath("$.slug").value("premium"))
            .andExpect(jsonPath("$.name").value("Plano Premium"))
            .andExpect(jsonPath("$.description").isNotEmpty())
            .andExpect(jsonPath("$.price").value(39.90))
            .andExpect(jsonPath("$.currency").value("BRL"));
    }

    @Test void unknownProductReturnsBodylessNotFound() throws Exception {
        mvc.perform(get("/products/missing")).andExpect(status().isNotFound()).andExpect(content().string(""));
    }

    @Test void paymentMethodsReturnsContract() throws Exception {
        mvc.perform(get("/payment-methods")).andExpect(status().isOk())
            .andExpect(jsonPath("$.paymentMethods.length()").value(3))
            .andExpect(jsonPath("$.paymentMethods[0].name").value("Cartão de Crédito"))
            .andExpect(jsonPath("$.paymentMethods[2].name").value("Pix"));
    }
}
