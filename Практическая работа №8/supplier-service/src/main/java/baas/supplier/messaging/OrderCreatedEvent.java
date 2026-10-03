package baas.supplier.messaging;

import java.util.List;
import java.util.UUID;

/**
 * Событие OrderCreated в том виде, в каком оно нужно Supplier Service: только товары
 * и количество. Остальные поля события (адрес, получатель) Jackson пропускает.
 */
public record OrderCreatedEvent(UUID eventId, String type, Long orderId, List<Item> items) {

    public static final String TYPE = "OrderCreated";

    public record Item(Long productId, int quantity) {
    }
}
