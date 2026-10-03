package baas.orders.web;

import baas.orders.domain.Order;
import baas.orders.domain.OrderItem;
import baas.orders.domain.OrderStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Тела запросов и ответы REST API Order Service. */
public final class Dto {

    private Dto() {
    }

    // ---------- Запросы ----------

    /** POST /api/orders. Цены не передаются: их сообщает Supplier Service. */
    public record CreateOrderRequest(
            @NotBlank(message = "укажите получателя") @Size(max = 200) String customerName,
            @NotBlank(message = "укажите адрес доставки") @Size(max = 500) String deliveryAddress,
            @Size(max = 1000) String comment,
            @NotEmpty(message = "добавьте хотя бы один товар") @Size(max = 20, message = "не больше 20 позиций")
            List<@Valid @NotNull ItemRequest> items) {
    }

    public record ItemRequest(
            @NotNull(message = "укажите productId") @Positive(message = "productId должен быть больше 0") Long productId,
            @NotNull(message = "укажите количество") @Min(value = 1, message = "количество должно быть не меньше 1")
            @Max(value = 100, message = "не больше 100 штук одного товара") Integer quantity) {
    }

    // ---------- Ответы ----------

    public record ItemView(Long productId, String productName, String supplierName,
                           BigDecimal price, int quantity, BigDecimal sum) {
        static ItemView from(OrderItem item) {
            return new ItemView(item.productId(), item.productName(), item.supplierName(), item.price(),
                    item.quantity(), item.price() == null ? null : item.sum());
        }
    }

    public record OrderView(Long id,
                            OrderStatus status,
                            String statusReason,
                            String customerName,
                            String deliveryAddress,
                            String comment,
                            List<ItemView> items,
                            BigDecimal total,
                            Long deliveryId,
                            String courier,
                            LocalDateTime createdAt,
                            LocalDateTime updatedAt) {
        public static OrderView from(Order order, List<OrderItem> items) {
            return new OrderView(order.id(), order.status(), order.statusReason(), order.customerName(),
                    order.deliveryAddress(), order.comment(), items.stream().map(ItemView::from).toList(),
                    order.total(), order.deliveryId(), order.courier(), order.createdAt(), order.updatedAt());
        }
    }
}
