package baas.orders.domain;

import java.util.Set;

/**
 * Статус заказа (Enum из модели BootcampLabs) и допустимые переходы между статусами.
 */
public enum OrderStatus {
    DRAFT,
    FIXED,
    CANCELED,
    ASSEMBLY,
    DELIVERY,
    COMPLETED;

    public Set<OrderStatus> nextStatuses() {
        return switch (this) {
            case DRAFT -> Set.of(FIXED, CANCELED);
            case FIXED -> Set.of(ASSEMBLY, CANCELED);
            case ASSEMBLY -> Set.of(DELIVERY, CANCELED);
            case DELIVERY -> Set.of(COMPLETED);
            case CANCELED, COMPLETED -> Set.of();
        };
    }

    public boolean canMoveTo(OrderStatus next) {
        return nextStatuses().contains(next);
    }
}
