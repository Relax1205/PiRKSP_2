package baas.delivery.client;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Заказ в том виде, в котором его отдаёт Order Service (GET /api/orders/{id}). */
public record OrderView(Long id,
                        Customer customer,
                        String comment,
                        LocalDateTime orderDateTime,
                        Address deliveryAddress,
                        String status,
                        List<Item> items,
                        BigDecimal total) {

    public record Customer(Long id, PersonalData personalData) {
    }

    public record PersonalData(String lastName, String firstName) {
    }

    public record Item(Long productId, String productName, BigDecimal price, int quantity, BigDecimal sum) {
    }

    public record Address(String city, String street, String building, String flat) {

        public String line() {
            String result = "г. " + city + ", " + street + ", д. " + building;
            return flat == null || flat.isBlank() ? result : result + ", кв. " + flat;
        }
    }

    public String recipient() {
        PersonalData person = customer.personalData();
        return person.lastName() + " " + person.firstName();
    }

    @JsonIgnore
    public boolean isCanceled() {
        return "CANCELED".equals(status);
    }
}
