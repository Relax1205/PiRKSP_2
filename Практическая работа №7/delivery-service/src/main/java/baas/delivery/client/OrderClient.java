package baas.delivery.client;

import baas.delivery.config.OrderServiceProperties;
import baas.delivery.error.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

/** Реактивный HTTP-клиент Order Service на базе WebClient. */
@Component
public class OrderClient {

    private static final Logger log = LoggerFactory.getLogger(OrderClient.class);

    private final WebClient webClient;
    private final OrderServiceProperties props;

    public OrderClient(WebClient orderServiceWebClient, OrderServiceProperties props) {
        this.webClient = orderServiceWebClient;
        this.props = props;
    }

    /**
     * Заказ по id. Ошибки переводятся в ответы Delivery Service:
     * 404 от Order Service -> 422, недоступен или 5xx -> 503.
     * При сбое соединения запрос повторяется props.retries() раз с нарастающей паузой.
     */
    public Mono<OrderView> getOrder(long orderId) {
        return webClient.get()
                .uri("/api/orders/{id}", orderId)
                .retrieve()
                .onStatus(HttpStatus.NOT_FOUND::equals, response -> Mono.error(Errors.orderNotFound(orderId)))
                .onStatus(HttpStatusCode::is5xxServerError, response -> Mono.error(Errors.orderServiceUnavailable(
                        "Order Service ответил " + response.statusCode().value())))
                .bodyToMono(OrderView.class)
                .doOnSubscribe(s -> log.info("WebClient -> GET {}/api/orders/{}", props.url(), orderId))
                .doOnNext(order -> log.info("WebClient <- заказ {} ({})", order.id(), order.status()))
                .retryWhen(Retry.backoff(props.retries(), Duration.ofMillis(300))
                        .filter(OrderClient::isConnectionProblem)
                        .doBeforeRetry(signal -> log.warn("Order Service недоступен, повтор №{}", signal.totalRetries() + 1))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .onErrorMap(OrderClient::isConnectionProblem, e -> Errors.orderServiceUnavailable(
                        "Order Service недоступен: " + e.getMessage()));
    }

    /** Ошибки соединения и таймауты, при которых имеет смысл повторить запрос. */
    private static boolean isConnectionProblem(Throwable e) {
        return e instanceof WebClientRequestException;
    }
}
