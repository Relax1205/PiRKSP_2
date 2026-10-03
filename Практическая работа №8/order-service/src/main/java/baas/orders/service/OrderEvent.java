package baas.orders.service;

import baas.orders.domain.OrderStatus;
import baas.orders.domain.OrderStatusChange;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.LocalDateTime;

/**
 * Событие изменения статуса заказа, которое уходит клиенту по SSE.
 *
 * @param seq номер записи в истории статусов; в SSE это поле id, по нему клиент
 *            может переподключиться и получить только новые события (Last-Event-ID)
 */
public record OrderEvent(long seq, long orderId, OrderStatus status, String details, LocalDateTime at) {

    public static OrderEvent of(OrderStatusChange change) {
        return new OrderEvent(change.id(), change.orderId(), change.status(), change.details(), change.createdAt());
    }

    /** После такого события поток по заказу завершается. */
    @JsonIgnore
    public boolean isFinal() {
        return status.isFinal();
    }
}
