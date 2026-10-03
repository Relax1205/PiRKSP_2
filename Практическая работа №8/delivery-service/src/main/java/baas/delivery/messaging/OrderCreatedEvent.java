package baas.delivery.messaging;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Событие OrderCreated в том виде, в каком оно нужно Delivery Service: кому и куда везти,
 * сумма заказа для расчёта стоимости доставки. Состав товаров Jackson пропускает.
 */
public record OrderCreatedEvent(UUID eventId,
                                String type,
                                Long orderId,
                                String customerName,
                                String deliveryAddress,
                                String comment,
                                BigDecimal total) {

    public static final String TYPE = "OrderCreated";
}
