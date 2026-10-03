package baas.delivery.domain;

import java.util.Set;

/** Статус доставки и допустимые переходы (как в смарт-контракте ПР №6 и в ПР №7). */
public enum DeliveryStatus {
    /** Доставка создана по событию OrderCreated. */
    CREATED,
    /** Назначен курьер. */
    ACCEPTED,
    /** Курьер везёт заказ. */
    IN_TRANSIT,
    /** Заказ вручён. */
    DELIVERED,
    /** Доставка отменена. */
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

    /** Следующий шаг курьера: CREATED → ACCEPTED → IN_TRANSIT → DELIVERED. */
    public DeliveryStatus nextStep() {
        return switch (this) {
            case CREATED -> ACCEPTED;
            case ACCEPTED -> IN_TRANSIT;
            case IN_TRANSIT -> DELIVERED;
            case DELIVERED, CANCELLED -> throw new IllegalStateException("Доставка уже завершена: " + this);
        };
    }
}
