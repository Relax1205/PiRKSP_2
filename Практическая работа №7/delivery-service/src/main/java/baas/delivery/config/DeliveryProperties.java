package baas.delivery.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;

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
                                 @Valid @NotNull Simulation simulation) {

    /**
     * @param step           пауза между сменами статуса
     * @param defaultCourier курьер, если он не был назначен заранее
     */
    public record Simulation(@NotNull Duration step, @NotBlank String defaultCourier) {
    }
}
