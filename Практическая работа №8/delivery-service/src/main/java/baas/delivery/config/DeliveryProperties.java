package baas.delivery.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Тарифы доставки и параметры имитации курьера (delivery.* в application.yml).
 *
 * @param baseCost      стоимость доставки
 * @param freeThreshold сумма заказа, начиная с которой доставка бесплатна
 */
@Validated
@ConfigurationProperties(prefix = "delivery")
public record DeliveryProperties(@NotNull BigDecimal baseCost,
                                 @NotNull BigDecimal freeThreshold,
                                 @Valid @NotNull Courier courier) {

    /**
     * @param simulation включена ли имитация курьера
     * @param step       пауза между шагами курьера
     * @param names      курьеры, назначаются по очереди
     */
    public record Courier(boolean simulation, @NotNull Duration step, @NotEmpty List<String> names) {
    }
}
