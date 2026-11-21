package br.com.globo.checkout.bff;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(info = @Info(title = "BFF de checkout", version = "1.0.0-SNAPSHOT"))
public class CheckoutBffApplication {
    public static void main(String[] args) { SpringApplication.run(CheckoutBffApplication.class, args); }
}
