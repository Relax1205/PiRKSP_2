package baas.delivery.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.LocalDateTime;

/**
 * Событие об изменении доставки, которое уходит клиенту по SSE.
 *
 * @param type имя статуса (CREATED, ACCEPTED, ...) или DELETED
 */
public record DeliveryEvent(Long deliveryId,
                            String type,
                            DeliveryStatus status,
                            String courier,
                            @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime at) {

    public static final String DELETED = "DELETED";

    public static DeliveryEvent of(Delivery delivery) {
        return new DeliveryEvent(delivery.id(), delivery.status().name(), delivery.status(),
                delivery.courier(), delivery.updatedAt());
    }

    public static DeliveryEvent deleted(Delivery delivery) {
        return new DeliveryEvent(delivery.id(), DELETED, delivery.status(), delivery.courier(), LocalDateTime.now());
    }

    /** После такого события поток по доставке завершается. */
    @JsonIgnore
    public boolean isLast() {
        return DELETED.equals(type) || status.isFinal();
    }
}
