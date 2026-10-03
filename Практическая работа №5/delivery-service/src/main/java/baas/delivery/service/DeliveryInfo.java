package baas.delivery.service;

import baas.delivery.domain.Address;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Информация о доставке заказа, сформированная Delivery Service. */
public record DeliveryInfo(Long orderId,
                           DeliveryStatus deliveryStatus,
                           OrderStatus orderStatus,
                           String recipient,
                           Address address,
                           String addressLine,
                           String comment,
                           int itemsCount,
                           BigDecimal orderTotal,
                           BigDecimal deliveryCost,
                           BigDecimal totalToPay,
                           LocalDateTime orderedAt,
                           LocalDateTime estimatedDeliveryAt) {
}
