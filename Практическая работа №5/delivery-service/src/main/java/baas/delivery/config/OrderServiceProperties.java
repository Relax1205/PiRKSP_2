package baas.delivery.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

/** Параметры подключения к Order Service (order-service.* в application.yml). */
@Validated
@ConfigurationProperties(prefix = "order-service")
public record OrderServiceProperties(@NotNull URI url,
                                     @NotNull Duration connectTimeout,
                                     @NotNull Duration readTimeout) {
}
