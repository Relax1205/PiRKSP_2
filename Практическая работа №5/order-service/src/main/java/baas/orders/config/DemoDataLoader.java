package baas.orders.config;

import baas.orders.domain.Address;
import baas.orders.domain.Customer;
import baas.orders.domain.Order;
import baas.orders.domain.OrderStatus;
import baas.orders.domain.PersonalData;
import baas.orders.domain.Product;
import baas.orders.repository.CustomerRepository;
import baas.orders.repository.OrderRepository;
import baas.orders.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Заполняет пустую БД демонстрационными данными (отключается SEED_DEMO_DATA=false). */
@Component
@ConditionalOnProperty(name = "app.seed-demo-data", havingValue = "true")
public class DemoDataLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoDataLoader.class);

    private final CustomerRepository customers;
    private final ProductRepository products;
    private final OrderRepository orders;

    public DemoDataLoader(CustomerRepository customers, ProductRepository products, OrderRepository orders) {
        this.customers = customers;
        this.products = products;
        this.orders = orders;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        if (orders.count() > 0) {
            log.info("БД уже содержит данные — демо-данные не загружаются");
            return;
        }

        List<Product> p = products.saveAll(List.of(
                new Product("Молоко 3,2%", "Пастеризованное, 1 л", new BigDecimal("89.90")),
                new Product("Хлеб бородинский", "Ржано-пшеничный, 400 г", new BigDecimal("65.00")),
                new Product("Яблоки Гала", "Свежие, 1 кг", new BigDecimal("159.00")),
                new Product("Сыр Российский", "Полутвёрдый, 300 г", new BigDecimal("329.00")),
                new Product("Кофе в зёрнах", "Арабика 100%, 1 кг", new BigDecimal("1490.00"))));

        List<Customer> c = customers.saveAll(List.of(
                new Customer(new PersonalData("Иванов", "Иван"),
                        new Address("Москва", "Тверская", "12", "45")),
                new Customer(new PersonalData("Петрова", "Анна"),
                        new Address("Москва", "Ленинский проспект", "30к2", "117")),
                new Customer(new PersonalData("Сидоров", "Пётр"),
                        new Address("Санкт-Петербург", "Невский проспект", "88", null))));

        LocalDateTime now = LocalDateTime.now();

        Order o1 = new Order(c.get(0), "Позвонить за 10 минут до приезда",
                c.get(0).getAddress().copy(), now.minusMinutes(5));
        o1.addItem(p.get(0), 2);
        o1.addItem(p.get(1), 1);

        Order o2 = new Order(c.get(1), "Домофон не работает",
                c.get(1).getAddress().copy(), now.minusMinutes(25));
        o2.addItem(p.get(4), 1);
        o2.addItem(p.get(3), 1);
        advance(o2, OrderStatus.FIXED, OrderStatus.ASSEMBLY);

        Order o3 = new Order(c.get(2), null,
                new Address("Санкт-Петербург", "Садовая", "5", "3"), now.minusMinutes(40));
        o3.addItem(p.get(2), 3);
        advance(o3, OrderStatus.FIXED, OrderStatus.ASSEMBLY, OrderStatus.DELIVERY);

        Order o4 = new Order(c.get(0), "Оставить у двери",
                c.get(0).getAddress().copy(), now.minusHours(3));
        o4.addItem(p.get(3), 2);
        advance(o4, OrderStatus.FIXED, OrderStatus.ASSEMBLY, OrderStatus.DELIVERY, OrderStatus.COMPLETED);

        Order o5 = new Order(c.get(1), null,
                c.get(1).getAddress().copy(), now.minusMinutes(15));
        o5.addItem(p.get(0), 1);
        o5.addItem(p.get(2), 2);
        advance(o5, OrderStatus.FIXED);

        orders.saveAll(List.of(o1, o2, o3, o4, o5));
        log.info("Загружены демо-данные: {} продуктов, {} клиентов, 5 заказов", p.size(), c.size());
    }

    private static void advance(Order order, OrderStatus... path) {
        for (OrderStatus status : path) {
            order.changeStatus(status);
        }
    }
}
