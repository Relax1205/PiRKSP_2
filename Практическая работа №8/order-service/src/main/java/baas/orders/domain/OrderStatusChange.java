package baas.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/** Запись истории статусов заказа: из неё SSE-поток отдаёт уже случившиеся события. */
@Table("order_status_history")
public record OrderStatusChange(@Id Long id,
                                Long orderId,
                                OrderStatus status,
                                String details,
                                LocalDateTime createdAt) {

    public static OrderStatusChange of(Order order, String details) {
        return new OrderStatusChange(null, order.id(), order.status(), details, LocalDateTime.now());
    }
}
