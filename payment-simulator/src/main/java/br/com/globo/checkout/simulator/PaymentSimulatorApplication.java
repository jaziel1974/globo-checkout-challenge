package br.com.globo.checkout.simulator;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(info = @Info(title = "Simulador de pagamento", version = "1.0.0-SNAPSHOT"))
public class PaymentSimulatorApplication {
    public static void main(String[] args) { SpringApplication.run(PaymentSimulatorApplication.class, args); }
}
