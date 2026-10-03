package baas.orders.service;

import baas.orders.domain.Address;
import baas.orders.domain.Customer;
import baas.orders.domain.Order;
import baas.orders.domain.OrderStatus;
import baas.orders.domain.Product;
import baas.orders.repository.CustomerRepository;
import baas.orders.repository.OrderRepository;
import baas.orders.repository.ProductRepository;
import baas.orders.web.Dto.CreateOrderRequest;
import baas.orders.web.Dto.OrderItemRequest;
import baas.orders.web.Dto.OrderResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final CustomerRepository customers;
    private final ProductRepository products;

    public OrderService(OrderRepository orders, CustomerRepository customers, ProductRepository products) {
        this.orders = orders;
        this.customers = customers;
        this.products = products;
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> findAll(Collection<OrderStatus> statuses) {
        List<Order> result = (statuses == null || statuses.isEmpty())
                ? orders.findAllByOrderById()
                : orders.findByStatusInOrderById(statuses);
        return result.stream().map(OrderResponse::from).toList();
    }

    @Transactional(readOnly = true)
    public OrderResponse findById(long id) {
        log.info("Запрос заказа {}", id);
        return OrderResponse.from(getOrder(id));
    }

    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        Customer customer = customers.findById(request.customerId())
                .orElseThrow(() -> notFound("Клиент " + request.customerId() + " не найден"));
        Address address = request.deliveryAddress() != null
                ? request.deliveryAddress()
                : customer.getAddress().copy();

        Order order = new Order(customer, request.comment(), address, LocalDateTime.now());
        for (OrderItemRequest item : request.items()) {
            Product product = products.findById(item.productId())
                    .orElseThrow(() -> notFound("Продукт " + item.productId() + " не найден"));
            order.addItem(product, item.quantity());
        }
        Order saved = orders.save(order);
        log.info("Создан заказ {} клиента {} на сумму {}", saved.getId(), customer.getId(), saved.total());
        return OrderResponse.from(saved);
    }

    @Transactional
    public OrderResponse changeStatus(long id, OrderStatus next) {
        Order order = getOrder(id);
        OrderStatus previous = order.getStatus();
        try {
            order.changeStatus(next);
        } catch (IllegalStateException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage());
        }
        log.info("Заказ {}: статус {} -> {}", id, previous, next);
        return OrderResponse.from(order);
    }

    private Order getOrder(long id) {
        return orders.findById(id).orElseThrow(() -> notFound("Заказ " + id + " не найден"));
    }

    static ResponseStatusException notFound(String message) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, message);
    }
}
