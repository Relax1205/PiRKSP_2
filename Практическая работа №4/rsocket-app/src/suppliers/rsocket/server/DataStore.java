package suppliers.rsocket.server;

import suppliers.rsocket.domain.Order;
import suppliers.rsocket.domain.Product;
import suppliers.rsocket.domain.Supplier;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Простое хранилище данных предметной области "АС Поставщиков" в памяти.
 * Данные соответствуют сущностям модуля "Front-end АС Поставщиков" BootcampLabs:
 * поставщики, товары и заказы.
 */
public class DataStore {

    private final Map<Integer, Supplier> suppliers = new LinkedHashMap<>();
    private final Map<Integer, Product> products = new LinkedHashMap<>();
    private final Map<Integer, Order> orders = new LinkedHashMap<>();
    private final AtomicInteger orderIdSequence = new AtomicInteger(100);

    public DataStore() {
        suppliers.put(1, new Supplier(1, "ООО \"ТехноСнаб\"", "7701234567", 4.7));
        suppliers.put(2, new Supplier(2, "ИП Кузнецов А.В.", "5029876543", 4.2));
        suppliers.put(3, new Supplier(3, "ЗАО \"МеталлТорг\"", "6612345678", 4.9));

        products.put(1, new Product(1, "Ноутбук ProBook X1", 1, 85000, 12));
        products.put(2, new Product(2, "Монитор 27\" UltraWide", 2, 42000, 30));
        products.put(3, new Product(3, "Серверная стойка 42U", 3, 67000, 5));
        products.put(4, new Product(4, "Комплект сетевых коммутаторов", 1, 128500, 8));
        products.put(5, new Product(5, "ИБП 3000VA", 2, 25900, 20));

        orders.put(1, new Order(1, 1, 2, "CREATED"));
        orders.put(2, new Order(2, 3, 1, "PROCESSING"));
        orderIdSequence.set(3);
    }

    public Supplier getSupplier(int id) {
        return suppliers.get(id);
    }

    public Product getProduct(int id) {
        return products.get(id);
    }

    public Iterable<Product> allProducts() {
        return products.values();
    }

    public Order getOrder(int id) {
        return orders.get(id);
    }

    public synchronized Order createOrder(int productId, int quantity) {
        int id = orderIdSequence.incrementAndGet();
        Order order = new Order(id, productId, quantity, "CREATED");
        orders.put(id, order);
        return order;
    }

    public synchronized Order advanceOrderStatus(int id) {
        Order order = orders.get(id);
        if (order == null) {
            return null;
        }
        String next;
        switch (order.getStatus()) {
            case "CREATED":
                next = "PROCESSING";
                break;
            case "PROCESSING":
                next = "SHIPPED";
                break;
            case "SHIPPED":
                next = "DELIVERED";
                break;
            default:
                next = order.getStatus();
        }
        order.setStatus(next);
        return order;
    }
}
