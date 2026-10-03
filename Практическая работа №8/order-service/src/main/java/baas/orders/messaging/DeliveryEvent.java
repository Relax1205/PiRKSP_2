package baas.orders.messaging;

import baas.orders.domain.OrderStatus;

import java.util.UUID;

/**
 * Событие Delivery Service из топика delivery-events (DeliveryCreated, DeliveryStatusChanged)
 * в том виде, в каком оно нужно Order Service. Статус — строка: новый статус доставки,
 * о котором Order Service ещё не знает, не сломает разбор сообщения.
 */
public record DeliveryEvent(UUID eventId, String type, Long deliveryId, Long orderId, String status, String courier) {

    /** Статус доставки → статус заказа (null — статус заказа не меняется). */
    public OrderStatus orderStatus() {
        return switch (status == null ? "" : status) {
            case "CREATED" -> OrderStatus.DELIVERY_CREATED;
            case "ACCEPTED" -> OrderStatus.COURIER_ASSIGNED;
            case "IN_TRANSIT" -> OrderStatus.IN_TRANSIT;
            case "DELIVERED" -> OrderStatus.DELIVERED;
            case "CANCELLED" -> OrderStatus.CANCELLED;
            default -> null;
        };
    }

    public String describe() {
        return switch (status == null ? "" : status) {
            case "CREATED" -> "Delivery Service создал доставку " + deliveryId;
            case "ACCEPTED" -> "Назначен курьер " + courier;
            case "IN_TRANSIT" -> "Курьер " + courier + " везёт заказ";
            case "DELIVERED" -> "Заказ вручён получателю, курьер " + courier;
            case "CANCELLED" -> "Доставка " + deliveryId + " отменена";
            default -> "Статус доставки " + status;
        };
    }
}
