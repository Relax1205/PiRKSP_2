package baas.orders.web;

import baas.orders.domain.OrderStatus;
import baas.orders.service.OrderEvent;
import baas.orders.service.OrderService;
import baas.orders.web.Dto.CreateOrderRequest;
import baas.orders.web.Dto.OrderView;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    /** События, после которых SSE-поток закрывается. */
    private static final Set<String> LAST_EVENTS = Set.of("DELIVERED", "REJECTED", "CANCELLED", "error");

    private final OrderService orderService;
    private final Duration keepAlive;

    public OrderController(OrderService orderService, @Value("${app.sse.keep-alive}") Duration keepAlive) {
        this.orderService = orderService;
        this.keepAlive = keepAlive;
    }

    /** Создание заказа: Order Service → Supplier Service (проверка товаров) → CONFIRMED → OrderCreated в Kafka. */
    @PostMapping
    public Mono<ResponseEntity<OrderView>> create(@Valid @RequestBody CreateOrderRequest request) {
        return orderService.create(request)
                .map(order -> ResponseEntity.created(URI.create("/api/orders/" + order.id())).body(order));
    }

    /** Несколько объектов — Flux. GET /api/orders?status=DELIVERED */
    @GetMapping
    public Flux<OrderView> findAll(@RequestParam(required = false) OrderStatus status) {
        return orderService.findAll(status);
    }

    /** Один объект — Mono. */
    @GetMapping("/{id}")
    public Mono<OrderView> findById(@PathVariable long id) {
        return orderService.findById(id);
    }

    /** История статусов обычным JSON-массивом. */
    @GetMapping("/{id}/history")
    public Flux<OrderEvent> history(@PathVariable long id) {
        return orderService.history(id);
    }

    /**
     * Поток изменений статуса заказа по Server-Sent Events: CREATED → CONFIRMED →
     * DELIVERY_CREATED → COURIER_ASSIGNED → IN_TRANSIT → DELIVERED. Каждые 15 с, пока
     * событий нет, уходит комментарий keep-alive, чтобы прокси не закрыли соединение.
     * Ошибка (например, нет заказа) приходит событием error: статус SSE-ответа уже 200.
     */
    @GetMapping(path = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> events(@PathVariable long id,
                                                @RequestHeader(name = "Last-Event-ID", required = false) Long lastEventId) {
        Flux<ServerSentEvent<Object>> events = orderService.events(id, lastEventId == null ? 0 : lastEventId)
                .map(event -> ServerSentEvent.<Object>builder(event)
                        .id(String.valueOf(event.seq()))
                        .event(event.status().name())
                        .build())
                .onErrorResume(ResponseStatusException.class, e -> Flux.just(ServerSentEvent.<Object>builder()
                        .event("error")
                        .data(Map.of("status", e.getStatusCode().value(), "detail", String.valueOf(e.getReason())))
                        .build()));
        Flux<ServerSentEvent<Object>> heartbeat = Flux.interval(keepAlive)
                .map(tick -> ServerSentEvent.<Object>builder().comment("keep-alive").build());
        return Flux.merge(events, heartbeat)
                .takeUntil(sse -> sse.event() != null && LAST_EVENTS.contains(sse.event()));
    }

    /** Демонстрация идемпотентности: повторно отправить OrderCreated этого заказа в Kafka. */
    @PostMapping("/{id}/republish")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Map<String, Object>> republish(@PathVariable long id) {
        return orderService.republish(id);
    }
}
