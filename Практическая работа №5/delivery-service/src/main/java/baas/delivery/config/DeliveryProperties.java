package baas.delivery.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

/**
 * Тарифы доставки (delivery.* в application.yml).
 *
 * @param baseCost         стоимость доставки
 * @param freeThreshold    сумма заказа, начиная с которой доставка бесплатна
 * @param estimatedMinutes плановое время доставки от момента оформления заказа
 */
@Validated
@ConfigurationProperties(prefix = "delivery")
public record DeliveryProperties(@NotNull BigDecimal baseCost,
                                 @NotNull BigDecimal freeThreshold,
                                 @Min(1) int estimatedMinutes) {
}
