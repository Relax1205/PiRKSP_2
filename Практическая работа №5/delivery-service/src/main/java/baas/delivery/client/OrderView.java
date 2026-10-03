package baas.delivery.client;

import baas.delivery.domain.Address;
import baas.delivery.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Заказ в том виде, в котором его отдаёт Order Service (GET /api/orders/{id}). */
public record OrderView(Long id,
                        Customer customer,
                        String comment,
                        LocalDateTime orderDateTime,
                        Address deliveryAddress,
                        OrderStatus status,
                        List<Item> items,
                        BigDecimal total) {

    public record Customer(Long id, PersonalData personalData) {
    }

    public record PersonalData(String lastName, String firstName) {
    }

    public record Item(Long productId, String productName, int quantity, BigDecimal sum) {
    }
}
