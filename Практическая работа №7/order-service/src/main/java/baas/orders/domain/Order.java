package baas.orders.domain;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Заказ в том же JSON-виде, что и в Order Service из ПР №5. */
public record Order(Long id,
                    Customer customer,
                    String comment,
                    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime orderDateTime,
                    Address deliveryAddress,
                    OrderStatus status,
                    List<Item> items,
                    BigDecimal total) {

    public record Customer(Long id, PersonalData personalData) {
    }

    public record PersonalData(String lastName, String firstName) {
    }

    public record Address(String city, String street, String building, String flat) {
    }

    public record Item(Long productId, String productName, BigDecimal price, int quantity, BigDecimal sum) {

        public static Item of(Long productId, String productName, String price, int quantity) {
            BigDecimal p = new BigDecimal(price);
            return new Item(productId, productName, p, quantity, p.multiply(BigDecimal.valueOf(quantity)));
        }
    }

    public static Order of(Long id, Customer customer, String comment, LocalDateTime orderDateTime,
                           Address address, OrderStatus status, List<Item> items) {
        BigDecimal total = items.stream().map(Item::sum).reduce(BigDecimal.ZERO, BigDecimal::add);
        return new Order(id, customer, comment, orderDateTime, address, status, items, total);
    }
}
