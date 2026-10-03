package baas.delivery.web;

import baas.delivery.domain.DeliveryStatus;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/** Тела запросов API доставок. */
public final class Dto {

    private Dto() {
    }

    /** POST /api/deliveries — данные доставки берутся из заказа в Order Service. */
    public record CreateDeliveryRequest(@NotNull(message = "укажите orderId")
                                        @Positive(message = "orderId должен быть больше 0") Long orderId) {
    }

    /** PUT /api/deliveries/{id} — изменяемые поля доставки целиком. */
    public record UpdateDeliveryRequest(String courier,
                                        @NotNull(message = "укажите стоимость")
                                        @DecimalMin(value = "0.00", message = "стоимость не может быть отрицательной") BigDecimal price,
                                        @NotNull(message = "укажите статус") DeliveryStatus status) {
    }
}
