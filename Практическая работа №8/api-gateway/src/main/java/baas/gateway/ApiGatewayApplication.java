package baas.gateway;

import baas.gateway.config.RequestId;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        RequestId.registerMdcPropagation();
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
