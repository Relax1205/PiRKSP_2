package baas.delivery.domain;

import java.util.Set;

/** Статус доставки и допустимые переходы (те же, что в смарт-контракте ПР №6). */
public enum DeliveryStatus {
    CREATED,
    ACCEPTED,
    IN_TRANSIT,
    DELIVERED,
    CANCELLED;

    public Set<DeliveryStatus> nextStatuses() {
        return switch (this) {
            case CREATED -> Set.of(ACCEPTED, CANCELLED);
            case ACCEPTED -> Set.of(IN_TRANSIT, CANCELLED);
            case IN_TRANSIT -> Set.of(DELIVERED);
            case DELIVERED, CANCELLED -> Set.of();
        };
    }

    public boolean canMoveTo(DeliveryStatus next) {
        return nextStatuses().contains(next);
    }

    public boolean isFinal() {
        return this == DELIVERED || this == CANCELLED;
    }

    /** Для этих статусов у доставки должен быть курьер. */
    public boolean requiresCourier() {
        return this == ACCEPTED || this == IN_TRANSIT || this == DELIVERED;
    }
}
