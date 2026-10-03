package baas.orders.web;

import baas.orders.domain.Order;
import baas.orders.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Component
public class OrderHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderHandler.class);

    private final OrderRepository repository;
    private final Duration responseDelay;

    public OrderHandler(OrderRepository repository, @Value("${orders.response-delay}") Duration responseDelay) {
        this.repository = repository;
        this.responseDelay = responseDelay;
    }

    /** GET /api/orders?status=ASSEMBLY&status=DELIVERY — фильтр по статусам необязателен. */
    public Mono<ServerResponse> findAll(ServerRequest request) {
        List<String> statuses = request.queryParams().getOrDefault("status", List.of());
        Flux<Order> orders = repository.findAll()
                .filter(order -> statuses.isEmpty() || statuses.contains(order.status().name()));
        return ServerResponse.ok().body(orders, Order.class);
    }

    /** GET /api/orders/{id}: 200 с заказом, 404 ProblemDetail, если заказа нет. */
    public Mono<ServerResponse> findById(ServerRequest request) {
        String rawId = request.pathVariable("id");
        return Mono.fromCallable(() -> Long.parseLong(rawId))
                .flatMap(repository::findById)
                .delayElement(responseDelay)
                .doOnNext(order -> log.info("Отдан заказ {} ({})", order.id(), order.status()))
                .flatMap(order -> ServerResponse.ok().bodyValue(order))
                .switchIfEmpty(Mono.defer(() -> problem(HttpStatus.NOT_FOUND, "Заказ " + rawId + " не найден")))
                .onErrorResume(NumberFormatException.class,
                        e -> problem(HttpStatus.BAD_REQUEST, "Идентификатор заказа должен быть числом: " + rawId));
    }

    private static Mono<ServerResponse> problem(HttpStatus status, String detail) {
        log.info("Ответ {}: {}", status.value(), detail);
        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyValue(ProblemDetail.forStatusAndDetail(status, detail));
    }
}
