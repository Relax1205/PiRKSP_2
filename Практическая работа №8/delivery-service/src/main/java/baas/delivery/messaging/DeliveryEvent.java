package baas.delivery.messaging;

import baas.delivery.domain.Delivery;
import baas.delivery.domain.DeliveryStatus;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Событие Delivery Service для топика delivery-events: доставка создана или сменила статус.
 * Только то, что нужно получателю: какая доставка, какого заказа, статус и курьер.
 */
public record DeliveryEvent(UUID eventId,
                            String type,
                            LocalDateTime occurredAt,
                            Long deliveryId,
                            Long orderId,
                            DeliveryStatus status,
                            String courier) {

    public static final String CREATED = "DeliveryCreated";
    public static final String STATUS_CHANGED = "DeliveryStatusChanged";

    public static DeliveryEvent of(String type, Delivery delivery) {
        return new DeliveryEvent(UUID.randomUUID(), type, LocalDateTime.now(), delivery.getId(),
                delivery.getOrderId(), delivery.getStatus(), delivery.getCourier());
    }
}
