package baas.delivery.domain;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Доставка заказа. Поля повторяют смарт-контракт ПР №6 (deliveryId, orderId,
 * customer, courier, price, status), плюс адрес и получатель из Order Service.
 */
public record Delivery(Long id,
                       Long orderId,
                       String recipient,
                       String address,
                       String courier,
                       BigDecimal price,
                       DeliveryStatus status,
                       @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime createdAt,
                       @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime updatedAt) {

    public Delivery withId(Long newId) {
        return new Delivery(newId, orderId, recipient, address, courier, price, status, createdAt, updatedAt);
    }

    public Delivery update(String newCourier, BigDecimal newPrice, DeliveryStatus newStatus) {
        return new Delivery(id, orderId, recipient, address, newCourier, newPrice, newStatus,
                createdAt, LocalDateTime.now());
    }
}
