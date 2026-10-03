package baas.orders.domain;

/**
 * Статус заказа. Основная цепочка:
 * CREATED → CONFIRMED → DELIVERY_CREATED → COURIER_ASSIGNED → IN_TRANSIT → DELIVERED.
 * Порядок констант важен: по нему проверяется, что статус меняется только вперёд.
 */
public enum OrderStatus {
    /** Заказ принят Order Service и сохранён, идёт проверка товаров. */
    CREATED,
    /** Supplier Service подтвердил наличие товаров, отправлено событие OrderCreated. */
    CONFIRMED,
    /** Delivery Service создал доставку (событие DeliveryCreated). */
    DELIVERY_CREATED,
    /** Назначен курьер. */
    COURIER_ASSIGNED,
    /** Курьер везёт заказ. */
    IN_TRANSIT,
    /** Заказ вручён. */
    DELIVERED,
    /** Товара нет в наличии или Supplier Service недоступен. */
    REJECTED,
    /** Доставка отменена. */
    CANCELLED;

    public boolean isFinal() {
        return this == DELIVERED || this == REJECTED || this == CANCELLED;
    }

    /**
     * Переходы только вперёд. Поэтому повторное или запоздавшее событие из Kafka
     * не может вернуть заказ в прошлый статус: обработка событий идемпотентна.
     */
    public boolean canMoveTo(OrderStatus next) {
        if (isFinal()) {
            return false;
        }
        return switch (next) {
            case CREATED -> false;
            case CONFIRMED, REJECTED -> this == CREATED;
            case CANCELLED -> this == CONFIRMED || this == DELIVERY_CREATED || this == COURIER_ASSIGNED;
            case DELIVERY_CREATED, COURIER_ASSIGNED, IN_TRANSIT, DELIVERED ->
                    this != CREATED && next.ordinal() > ordinal();
        };
    }
}
