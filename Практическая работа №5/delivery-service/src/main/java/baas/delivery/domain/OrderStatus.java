package baas.delivery.domain;

/** Статус заказа в контракте Order Service. */
public enum OrderStatus {
    DRAFT,
    FIXED,
    CANCELED,
    ASSEMBLY,
    DELIVERY,
    COMPLETED
}
