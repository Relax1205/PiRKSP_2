package baas.delivery.service;

import baas.delivery.client.OrderView;
import baas.delivery.domain.Delivery;

/**
 * Доставка вместе с заказом из Order Service.
 * Если Order Service недоступен, order = null, а причина — в orderError.
 */
public record DeliveryDetails(Delivery delivery, OrderView order, String orderError) {

    public static DeliveryDetails withOrder(Delivery delivery, OrderView order) {
        return new DeliveryDetails(delivery, order, null);
    }

    public static DeliveryDetails withoutOrder(Delivery delivery, String error) {
        return new DeliveryDetails(delivery, null, error);
    }
}
