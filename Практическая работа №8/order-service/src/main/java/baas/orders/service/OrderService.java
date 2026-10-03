package baas.orders.service;

import baas.orders.client.ProductView;
import baas.orders.client.SupplierClient;
import baas.orders.client.SupplierUnavailableException;
import baas.orders.config.RequestId;
import baas.orders.domain.Order;
import baas.orders.domain.OrderItem;
import baas.orders.domain.OrderStatus;
import baas.orders.domain.OrderStatusChange;
import baas.orders.domain.OutboxEvent;
import baas.orders.messaging.DeliveryEvent;
import baas.orders.messaging.OrderCreatedEvent;
import baas.orders.repository.OrderItemRepository;
import baas.orders.repository.OrderRepository;
import baas.orders.repository.OrderStatusChangeRepository;
import baas.orders.repository.OutboxRepository;
import baas.orders.web.Dto.CreateOrderRequest;
import baas.orders.web.Dto.ItemRequest;
import baas.orders.web.Dto.OrderView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Вся логика — реактивные цепочки. Транзакции R2DBC задаются через TransactionalOperator. */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final OrderItemRepository items;
    private final OrderStatusChangeRepository history;
    private final OutboxRepository outbox;
    private final SupplierClient supplierClient;
    private final OrderEventBus eventBus;
    private final TransactionalOperator tx;
    private final ObjectMapper objectMapper;
    private final String orderEventsTopic;

    public OrderService(OrderRepository orders, OrderItemRepository items, OrderStatusChangeRepository history,
                        OutboxRepository outbox, SupplierClient supplierClient, OrderEventBus eventBus,
                        ReactiveTransactionManager transactionManager, ObjectMapper objectMapper,
                        @Value("${app.kafka.order-events-topic}") String orderEventsTopic) {
        this.orders = orders;
        this.items = items;
        this.history = history;
        this.outbox = outbox;
        this.supplierClient = supplierClient;
        this.eventBus = eventBus;
        this.tx = TransactionalOperator.create(transactionManager);
        this.objectMapper = objectMapper;
        this.orderEventsTopic = orderEventsTopic;
    }

    /** Новое состояние заказа и запись истории о переходе. */
    private record Transition(Order order, OrderStatusChange change) {
    }

    // ---------- Создание заказа ----------

    /**
     * 1) заказ сохраняется в статусе CREATED;
     * 2) синхронный запрос в Supplier Service: есть ли товары и по какой цене;
     * 3) товары есть — CONFIRMED и событие OrderCreated в outbox (одна транзакция);
     *    товара не хватает или Supplier Service недоступен — REJECTED и ошибка 409/422/503.
     * Транзакция БД не держится открытой, пока идёт HTTP-запрос к другому сервису.
     */
    public Mono<OrderView> create(CreateOrderRequest request) {
        Map<Long, Integer> quantities = quantities(request.items());
        return Mono.deferContextual(ctx -> register(request, quantities, ctx.getOrDefault(RequestId.CONTEXT_KEY, null)))
                .flatMap(order -> supplierClient.findProducts(quantities.keySet())
                        .flatMap(products -> confirmOrReject(order, quantities, products))
                        .onErrorResume(SupplierUnavailableException.class, e -> reject(order,
                                HttpStatus.SERVICE_UNAVAILABLE, "Supplier Service недоступен: " + e.getMessage())));
    }

    private Mono<Order> register(CreateOrderRequest request, Map<Long, Integer> quantities, String requestId) {
        Order draft = Order.create(request.customerName().trim(), request.deliveryAddress().trim(),
                blankToNull(request.comment()), requestId);
        return tx.transactional(orders.save(draft)
                        .flatMap(order -> items.saveAll(quantities.entrySet().stream()
                                        .map(e -> OrderItem.requested(order.id(), e.getKey(), e.getValue()))
                                        .toList())
                                .then(history.save(OrderStatusChange.of(order,
                                        "Заказ принят Order Service, позиций: " + quantities.size())))
                                .map(change -> new Transition(order, change))))
                .doOnNext(t -> log.info("Заказ {} зарегистрирован (CREATED): {}, позиций {}",
                        t.order().id(), t.order().customerName(), quantities.size()))
                .doOnNext(this::publish)
                .map(Transition::order);
    }

    private Mono<OrderView> confirmOrReject(Order order, Map<Long, Integer> quantities, List<ProductView> products) {
        Map<Long, ProductView> byId = products.stream().collect(Collectors.toMap(ProductView::id, Function.identity()));
        for (Map.Entry<Long, Integer> wanted : quantities.entrySet()) {
            ProductView product = byId.get(wanted.getKey());
            if (product == null) {
                return reject(order, HttpStatus.UNPROCESSABLE_ENTITY,
                        "Товар " + wanted.getKey() + " не найден в каталоге Supplier Service");
            }
            if (product.stock() < wanted.getValue()) {
                return reject(order, HttpStatus.CONFLICT, "Недостаточно товара «" + product.name() + "»: заказано "
                        + wanted.getValue() + ", на складе " + product.stock());
            }
        }
        return confirm(order, byId);
    }

    /** Цены фиксируются в заказе, событие OrderCreated пишется в outbox в той же транзакции. */
    private Mono<OrderView> confirm(Order order, Map<Long, ProductView> products) {
        return items.findByOrderIdOrderById(order.id())
                .map(item -> item.confirm(products.get(item.productId())))
                .collectList()
                .flatMap(confirmedItems -> {
                    BigDecimal total = confirmedItems.stream().map(OrderItem::sum).reduce(BigDecimal.ZERO, BigDecimal::add);
                    return tx.transactional(items.saveAll(confirmedItems)
                                    .then(orders.save(order.confirm(total)))
                                    .flatMap(saved -> history.save(OrderStatusChange.of(saved,
                                                    "Supplier Service подтвердил наличие товаров, сумма " + total + " ₽"))
                                            .flatMap(change -> outbox.save(orderCreatedEvent(saved, confirmedItems))
                                                    .thenReturn(new Transition(saved, change)))))
                            .doOnNext(t -> log.info("Заказ {} подтверждён (CONFIRMED) на сумму {} ₽ — событие OrderCreated записано в outbox",
                                    t.order().id(), total))
                            .doOnNext(this::publish)
                            .map(t -> OrderView.from(t.order(), confirmedItems));
                });
    }

    private Mono<OrderView> reject(Order order, HttpStatus status, String reason) {
        return tx.transactional(orders.save(order.reject(reason))
                        .flatMap(saved -> history.save(OrderStatusChange.of(saved, reason))
                                .map(change -> new Transition(saved, change))))
                .doOnNext(t -> log.warn("Заказ {} отклонён (REJECTED): {}", order.id(), reason))
                .doOnNext(this::publish)
                .then(Mono.error(new OrderRejectedException(status, order.id(), reason)));
    }

    private OutboxEvent orderCreatedEvent(Order order, List<OrderItem> orderItems) {
        OrderCreatedEvent event = OrderCreatedEvent.of(order, orderItems);
        return OutboxEvent.of(event.eventId(), OrderCreatedEvent.TYPE, orderEventsTopic,
                String.valueOf(order.id()), toJson(event), order.requestId(), order.id());
    }

    // ---------- События Delivery Service (Kafka) ----------

    /**
     * Статус доставки → статус заказа. Статус меняется только вперёд (OrderStatus.canMoveTo),
     * поэтому повторно доставленное или устаревшее событие ничего не меняет.
     */
    public Mono<Void> applyDeliveryEvent(DeliveryEvent event) {
        OrderStatus target = event.orderStatus();
        return orders.findById(event.orderId())
                .switchIfEmpty(Mono.fromRunnable(() -> log.warn("Заказ {} не найден — событие {} пропущено",
                        event.orderId(), event.eventId())))
                .flatMap(order -> {
                    if (target == null || !order.status().canMoveTo(target)) {
                        log.info("Заказ {} уже в статусе {}: событие {} ({}) статус не меняет — повтор или устаревшее событие",
                                order.id(), order.status(), event.type(), event.status());
                        return Mono.<Order>empty();
                    }
                    String courier = event.courier() != null ? event.courier() : order.courier();
                    return changeStatus(order.withDelivery(event.deliveryId(), courier), target, event.describe());
                })
                .then();
    }

    private Mono<Order> changeStatus(Order order, OrderStatus next, String details) {
        OrderStatus previous = order.status();
        String reason = next == OrderStatus.CANCELLED ? details : null;
        return tx.transactional(orders.save(order.withStatus(next, reason))
                        .flatMap(saved -> history.save(OrderStatusChange.of(saved, details))
                                .map(change -> new Transition(saved, change))))
                .doOnNext(t -> log.info("Заказ {}: {} -> {} ({})", order.id(), previous, next, details))
                .doOnNext(this::publish)
                .map(Transition::order);
    }

    // ---------- Чтение ----------

    public Flux<OrderView> findAll(OrderStatus status) {
        Flux<Order> found = status == null ? orders.findAllByOrderByIdDesc() : orders.findByStatusOrderByIdDesc(status);
        return found.concatMap(this::view);
    }

    public Mono<OrderView> findById(long id) {
        return getOrder(id).flatMap(this::view);
    }

    public Flux<OrderEvent> history(long id) {
        return getOrder(id).flatMapMany(order -> history.findByOrderIdOrderById(id).map(OrderEvent::of));
    }

    /**
     * Поток изменений статуса для SSE: сначала история из БД, затем новые события из шины.
     * Чтобы не потерять событие, случившееся между чтением истории и подпиской, на шину
     * подписываемся ДО чтения истории (replay буферизует события), а повторы отсекаем по seq.
     * Поток завершается на конечном статусе: DELIVERED, REJECTED или CANCELLED.
     */
    public Flux<OrderEvent> events(long orderId, long afterSeq) {
        return getOrder(orderId)
                .flatMapMany(order -> Flux.defer(() -> {
                    AtomicReference<Disposable> liveConnection = new AtomicReference<>();
                    Flux<OrderEvent> live = eventBus.events()
                            .filter(event -> event.orderId() == orderId)
                            .replay()
                            .autoConnect(0, liveConnection::set);
                    return history.findByOrderIdOrderById(orderId)
                            .map(OrderEvent::of)
                            .collectList()
                            .flatMapMany(past -> {
                                long lastSeq = past.isEmpty() ? 0 : past.getLast().seq();
                                return Flux.fromIterable(past).concatWith(live.filter(event -> event.seq() > lastSeq));
                            })
                            .takeUntil(OrderEvent::isFinal)
                            .filter(event -> event.seq() > afterSeq)
                            .doFinally(signal -> {
                                Disposable connection = liveConnection.get();
                                if (connection != null) {
                                    connection.dispose();
                                }
                            });
                }))
                .doOnSubscribe(s -> log.info("SSE: подписка на заказ {}", orderId))
                .doOnCancel(() -> log.info("SSE: клиент отключился от заказа {}", orderId))
                .doOnComplete(() -> log.info("SSE: поток заказа {} завершён", orderId));
    }

    // ---------- Демонстрация идемпотентности ----------

    /**
     * Повторно отправляет в Kafka уже отправленное событие OrderCreated с тем же eventId —
     * так выглядит повторная доставка сообщения (at-least-once). Потребители должны его пропустить.
     */
    public Mono<Map<String, Object>> republish(long orderId) {
        return outbox.findFirstByOrderIdAndEventType(orderId, OrderCreatedEvent.TYPE)
                .switchIfEmpty(Mono.error(() -> new ResponseStatusException(HttpStatus.CONFLICT,
                        "Для заказа " + orderId + " нет события OrderCreated: заказ не найден или не подтверждён")))
                .flatMap(event -> outbox.save(event.resend()))
                .doOnNext(event -> log.warn("Демонстрация идемпотентности: OrderCreated {} заказа {} будет отправлено в Kafka повторно",
                        event.eventId(), orderId))
                .map(event -> Map.of(
                        "orderId", orderId,
                        "eventId", event.eventId(),
                        "message", "Событие OrderCreated будет отправлено в Kafka повторно с тем же eventId"));
    }

    // ---------- Вспомогательное ----------

    private Mono<Order> getOrder(long id) {
        return orders.findById(id)
                .switchIfEmpty(Mono.error(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заказ " + id + " не найден")));
    }

    private Mono<OrderView> view(Order order) {
        return items.findByOrderIdOrderById(order.id()).collectList().map(list -> OrderView.from(order, list));
    }

    /** Публикация в шину — только после коммита транзакции: SSE не покажет то, чего нет в БД. */
    private void publish(Transition transition) {
        eventBus.publish(OrderEvent.of(transition.change()));
    }

    /** Одинаковые товары в запросе складываются: [{1, 2}, {1, 1}] → {1: 3}. */
    private static Map<Long, Integer> quantities(List<ItemRequest> requested) {
        Map<Long, Integer> result = new LinkedHashMap<>();
        requested.forEach(item -> result.merge(item.productId(), item.quantity(), Integer::sum));
        return result;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Не удалось сериализовать событие", e);
        }
    }
}
