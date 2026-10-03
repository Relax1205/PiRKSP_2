package baas.orders;

import baas.orders.config.RequestId;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderServiceApplication {

    public static void main(String[] args) {
        RequestId.registerMdcPropagation();
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
