package baas.orders.messaging;

import baas.orders.domain.Order;
import baas.orders.domain.OrderItem;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Событие OrderCreated (топик order-events). Только то, что нужно потребителям:
 * Delivery Service — кому и куда везти и сумма для тарифа; Supplier Service — какие
 * товары и сколько списать. Цен позиций, названий и ФИО по частям в событии нет.
 */
public record OrderCreatedEvent(UUID eventId,
                                String type,
                                LocalDateTime occurredAt,
                                Long orderId,
                                String customerName,
                                String deliveryAddress,
                                String comment,
                                BigDecimal total,
                                List<Item> items) {

    public static final String TYPE = "OrderCreated";

    public record Item(Long productId, int quantity) {
    }

    public static OrderCreatedEvent of(Order order, List<OrderItem> items) {
        return new OrderCreatedEvent(UUID.randomUUID(), TYPE, LocalDateTime.now(), order.id(),
                order.customerName(), order.deliveryAddress(), order.comment(), order.total(),
                items.stream().map(item -> new Item(item.productId(), item.quantity())).toList());
    }
}
