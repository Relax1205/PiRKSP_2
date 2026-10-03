package baas.orders.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

/**
 * Подключение к Supplier Service (supplier-service.* в application.yml).
 * Таймаут ответа, повторы и circuit breaker настраиваются в resilience4j.*.
 */
@Validated
@ConfigurationProperties(prefix = "supplier-service")
public record SupplierServiceProperties(@NotNull URI url, @NotNull Duration connectTimeout) {
}
