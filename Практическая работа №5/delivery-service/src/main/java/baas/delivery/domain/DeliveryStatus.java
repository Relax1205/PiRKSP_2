package baas.delivery.domain;

/** Статус доставки, вычисляемый из статуса заказа. */
public enum DeliveryStatus {
    /** Заказ ещё не передан в сборку (DRAFT, FIXED). */
    PENDING,
    /** Заказ собирается (ASSEMBLY). */
    ASSEMBLING,
    /** Курьер в пути (DELIVERY). */
    IN_TRANSIT,
    /** Заказ доставлен (COMPLETED). */
    DELIVERED,
    /** Заказ отменён (CANCELED). */
    CANCELED;

    public static DeliveryStatus of(OrderStatus status) {
        return switch (status) {
            case DRAFT, FIXED -> PENDING;
            case ASSEMBLY -> ASSEMBLING;
            case DELIVERY -> IN_TRANSIT;
            case COMPLETED -> DELIVERED;
            case CANCELED -> CANCELED;
        };
    }

    public boolean isFinal() {
        return this == DELIVERED || this == CANCELED;
    }
}
