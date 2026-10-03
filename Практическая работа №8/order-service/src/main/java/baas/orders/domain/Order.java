package baas.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Заказ (таблица orders). Неизменяемый record: каждое изменение — новый объект,
 * который сохраняется через R2DBC. @Version — оптимистическая блокировка.
 */
@Table("orders")
public record Order(@Id Long id,
                    String customerName,
                    String deliveryAddress,
                    String comment,
                    OrderStatus status,
                    String statusReason,
                    BigDecimal total,
                    Long deliveryId,
                    String courier,
                    String requestId,
                    LocalDateTime createdAt,
                    LocalDateTime updatedAt,
                    @Version Long version) {

    public static Order create(String customerName, String deliveryAddress, String comment, String requestId) {
        LocalDateTime now = LocalDateTime.now();
        return new Order(null, customerName, deliveryAddress, comment, OrderStatus.CREATED, null, null,
                null, null, requestId, now, now, null);
    }

    public Order confirm(BigDecimal total) {
        return new Order(id, customerName, deliveryAddress, comment, OrderStatus.CONFIRMED, null, total,
                deliveryId, courier, requestId, createdAt, LocalDateTime.now(), version);
    }

    public Order reject(String reason) {
        return withStatus(OrderStatus.REJECTED, reason);
    }

    public Order withStatus(OrderStatus newStatus, String reason) {
        return new Order(id, customerName, deliveryAddress, comment, newStatus, reason, total,
                deliveryId, courier, requestId, createdAt, LocalDateTime.now(), version);
    }

    public Order withDelivery(Long newDeliveryId, String newCourier) {
        return new Order(id, customerName, deliveryAddress, comment, status, statusReason, total,
                newDeliveryId, newCourier, requestId, createdAt, updatedAt, version);
    }
}
