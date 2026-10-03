package baas.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Transactional outbox: событие записывается в БД в той же транзакции, что и заказ,
 * а в Kafka его отправляет OutboxRelay. Так событие не теряется, даже если Kafka
 * в момент подтверждения заказа недоступна.
 */
@Table("outbox")
public record OutboxEvent(@Id Long id,
                          UUID eventId,
                          String eventType,
                          String topic,
                          String messageKey,
                          String payload,
                          String requestId,
                          Long orderId,
                          LocalDateTime createdAt,
                          LocalDateTime sentAt) {

    public static OutboxEvent of(UUID eventId, String eventType, String topic, String messageKey,
                                 String payload, String requestId, Long orderId) {
        return new OutboxEvent(null, eventId, eventType, topic, messageKey, payload, requestId, orderId,
                LocalDateTime.now(), null);
    }

    public OutboxEvent markSent() {
        return new OutboxEvent(id, eventId, eventType, topic, messageKey, payload, requestId, orderId,
                createdAt, LocalDateTime.now());
    }

    /** Для демонстрации идемпотентности: то же сообщение (тот же eventId) уйдёт в Kafka ещё раз. */
    public OutboxEvent resend() {
        return new OutboxEvent(id, eventId, eventType, topic, messageKey, payload, requestId, orderId,
                createdAt, null);
    }
}
