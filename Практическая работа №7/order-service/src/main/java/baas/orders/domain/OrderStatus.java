package baas.orders.domain;

/** Статус заказа (Enum из модели BootcampLabs, как в ПР №5). */
public enum OrderStatus {
    DRAFT,
    FIXED,
    CANCELED,
    ASSEMBLY,
    DELIVERY,
    COMPLETED
}
