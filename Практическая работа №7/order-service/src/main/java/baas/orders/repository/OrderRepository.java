package baas.orders.repository;

import baas.orders.domain.Order;
import baas.orders.domain.Order.Address;
import baas.orders.domain.Order.Customer;
import baas.orders.domain.Order.Item;
import baas.orders.domain.Order.PersonalData;
import baas.orders.domain.OrderStatus;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Заказы в памяти с реактивным интерфейсом (Mono/Flux).
 * Демо-данные совпадают с ПР №5: те же клиенты, товары и статусы.
 */
@Repository
public class OrderRepository {

    private final Map<Long, Order> orders = new TreeMap<>();

    public OrderRepository() {
        Customer ivanov = new Customer(1L, new PersonalData("Иванов", "Иван"));
        Customer petrova = new Customer(2L, new PersonalData("Петрова", "Анна"));
        Customer sidorov = new Customer(3L, new PersonalData("Сидоров", "Пётр"));
        Address tverskaya = new Address("Москва", "Тверская", "12", "45");
        Address leninsky = new Address("Москва", "Ленинский проспект", "30к2", "117");
        LocalDateTime now = LocalDateTime.now();

        add(Order.of(1L, ivanov, "Позвонить за 10 минут до приезда", now.minusMinutes(5), tverskaya,
                OrderStatus.DRAFT, List.of(
                        Item.of(1L, "Молоко 3,2%", "89.90", 2),
                        Item.of(2L, "Хлеб бородинский", "65.00", 1))));
        add(Order.of(2L, petrova, "Домофон не работает", now.minusMinutes(25), leninsky,
                OrderStatus.ASSEMBLY, List.of(
                        Item.of(5L, "Кофе в зёрнах", "1490.00", 1),
                        Item.of(4L, "Сыр Российский", "329.00", 1))));
        add(Order.of(3L, sidorov, null, now.minusMinutes(40),
                new Address("Санкт-Петербург", "Садовая", "5", "3"),
                OrderStatus.DELIVERY, List.of(
                        Item.of(3L, "Яблоки Гала", "159.00", 3))));
        add(Order.of(4L, ivanov, "Оставить у двери", now.minusHours(3), tverskaya,
                OrderStatus.COMPLETED, List.of(
                        Item.of(4L, "Сыр Российский", "329.00", 2))));
        add(Order.of(5L, petrova, null, now.minusMinutes(15), leninsky,
                OrderStatus.FIXED, List.of(
                        Item.of(1L, "Молоко 3,2%", "89.90", 1),
                        Item.of(3L, "Яблоки Гала", "159.00", 2))));
        add(Order.of(6L, sidorov, "Клиент передумал", now.minusHours(1),
                new Address("Санкт-Петербург", "Невский проспект", "88", null),
                OrderStatus.CANCELED, List.of(
                        Item.of(2L, "Хлеб бородинский", "65.00", 2))));
    }

    private void add(Order order) {
        orders.put(order.id(), order);
    }

    public Flux<Order> findAll() {
        return Flux.fromIterable(orders.values());
    }

    /** Пустой Mono, если заказа нет. */
    public Mono<Order> findById(long id) {
        return Mono.justOrEmpty(orders.get(id));
    }
}
