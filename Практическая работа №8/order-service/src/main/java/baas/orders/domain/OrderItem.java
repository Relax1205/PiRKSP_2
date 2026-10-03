package baas.orders.domain;

import baas.orders.client.ProductView;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;

/**
 * Позиция заказа. При создании известны только товар и количество; название, поставщик
 * и цена копируются из ответа Supplier Service, когда заказ подтверждается.
 */
@Table("order_items")
public record OrderItem(@Id Long id,
                        Long orderId,
                        Long productId,
                        int quantity,
                        String productName,
                        String supplierName,
                        BigDecimal price) {

    public static OrderItem requested(Long orderId, Long productId, int quantity) {
        return new OrderItem(null, orderId, productId, quantity, null, null, null);
    }

    public OrderItem confirm(ProductView product) {
        return new OrderItem(id, orderId, productId, quantity, product.name(), product.supplier().name(), product.price());
    }

    public BigDecimal sum() {
        return price == null ? BigDecimal.ZERO : price.multiply(BigDecimal.valueOf(quantity));
    }
}
