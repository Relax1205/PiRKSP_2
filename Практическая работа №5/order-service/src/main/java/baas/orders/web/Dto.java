package baas.orders.web;

import baas.orders.domain.Address;
import baas.orders.domain.Customer;
import baas.orders.domain.Order;
import baas.orders.domain.OrderList;
import baas.orders.domain.OrderStatus;
import baas.orders.domain.PersonalData;
import baas.orders.domain.Product;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** DTO REST API Order Service (запросы и ответы). */
public final class Dto {

    private Dto() {
    }

    // ---------- Запросы ----------

    public record CustomerRequest(@Valid @NotNull PersonalData personalData,
                                  @Valid @NotNull Address address) {
    }

    public record ProductRequest(@NotBlank String name,
                                 String description,
                                 @NotNull @DecimalMin("0.00") BigDecimal price) {
    }

    public record OrderItemRequest(@NotNull Long productId,
                                   @NotNull @Min(1) Integer quantity) {
    }

    /** Если deliveryAddress не передан — берётся основной адрес клиента. */
    public record CreateOrderRequest(@NotNull Long customerId,
                                     String comment,
                                     @Valid Address deliveryAddress,
                                     @NotEmpty List<@Valid OrderItemRequest> items) {
    }

    public record ChangeStatusRequest(@NotNull OrderStatus status) {
    }

    // ---------- Ответы ----------

    public record CustomerResponse(Long id, PersonalData personalData, Address address) {
        public static CustomerResponse from(Customer c) {
            return new CustomerResponse(c.getId(), c.getPersonalData(), c.getAddress());
        }
    }

    public record ProductResponse(Long id, String name, String description, BigDecimal price) {
        public static ProductResponse from(Product p) {
            return new ProductResponse(p.getId(), p.getName(), p.getDescription(), p.getPrice());
        }
    }

    public record OrderItemResponse(Long id, Long productId, String productName,
                                    BigDecimal price, int quantity, BigDecimal sum) {
        public static OrderItemResponse from(OrderList item) {
            Product p = item.getProduct();
            return new OrderItemResponse(item.getId(), p.getId(), p.getName(),
                    p.getPrice(), item.getQuantity(), item.sum());
        }
    }

    public record OrderResponse(Long id,
                                CustomerResponse customer,
                                String comment,
                                LocalDateTime orderDateTime,
                                Address deliveryAddress,
                                OrderStatus status,
                                List<OrderItemResponse> items,
                                BigDecimal total) {
        public static OrderResponse from(Order o) {
            return new OrderResponse(o.getId(),
                    CustomerResponse.from(o.getCustomer()),
                    o.getComment(),
                    o.getOrderDateTime(),
                    o.getDeliveryAddress(),
                    o.getStatus(),
                    o.getOrderListList().stream().map(OrderItemResponse::from).toList(),
                    o.total());
        }
    }
}
