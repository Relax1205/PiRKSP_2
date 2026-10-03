package baas.orders.client;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Синхронное (запрос — ответ) взаимодействие с Supplier Service через неблокирующий WebClient.
 * Вызов защищён Resilience4j, порядок обёрток снаружи внутрь:
 * Retry → CircuitBreaker → TimeLimiter → HTTP-запрос.
 * Таймаут ограничивает каждую попытку, circuit breaker считает каждую попытку,
 * Retry повторяет попытки, но не повторяет отказ открытого circuit breaker.
 */
@Component
public class SupplierClient {

    private static final Logger log = LoggerFactory.getLogger(SupplierClient.class);

    public static final String NAME = "supplierService";

    private final WebClient webClient;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final TimeLimiter timeLimiter;
    private final SupplierFailurePredicate isFailure = new SupplierFailurePredicate();

    public SupplierClient(WebClient supplierWebClient, CircuitBreakerRegistry circuitBreakers,
                          RetryRegistry retries, TimeLimiterRegistry timeLimiters) {
        this.webClient = supplierWebClient;
        this.circuitBreaker = circuitBreakers.circuitBreaker(NAME);
        this.retry = retries.retry(NAME);
        this.timeLimiter = timeLimiters.timeLimiter(NAME);

        circuitBreaker.getEventPublisher().onStateTransition(event -> log.warn("Circuit breaker {}: {} -> {}",
                NAME, event.getStateTransition().getFromState(), event.getStateTransition().getToState()));
        retry.getEventPublisher().onRetry(event -> log.warn("Supplier Service: попытка {} не удалась ({}), повтор через {} мс",
                event.getNumberOfRetryAttempts(), describe(event.getLastThrowable()), event.getWaitInterval().toMillis()));
    }

    /**
     * GET /api/products?ids=1,5 — цены и остатки товаров заказа.
     * GET не меняет данные, поэтому его безопасно повторять.
     */
    public Mono<List<ProductView>> findProducts(Collection<Long> ids) {
        String idsParam = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
        return webClient.get()
                .uri(builder -> builder.path("/api/products").queryParam("ids", idsParam).build())
                .retrieve()
                .bodyToFlux(ProductView.class)
                .collectList()
                .doOnSubscribe(s -> log.info("→ Supplier Service: GET /api/products?ids={}", idsParam))
                .transformDeferred(TimeLimiterOperator.of(timeLimiter))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .transformDeferred(RetryOperator.of(retry))
                .doOnNext(products -> log.info("← Supplier Service: получено товаров {} из {}", products.size(), ids.size()))
                .onErrorMap(e -> e instanceof CallNotPermittedException || isFailure.test(e),
                        e -> new SupplierUnavailableException(describe(e), e));
    }

    public CircuitBreaker.State circuitBreakerState() {
        return circuitBreaker.getState();
    }

    private String describe(Throwable e) {
        if (e instanceof CallNotPermittedException) {
            return "circuit breaker открыт — Supplier Service недавно не отвечал, запрос не отправлялся";
        }
        if (e instanceof TimeoutException) {
            return "Supplier Service не ответил за " + timeLimiter.getTimeLimiterConfig().getTimeoutDuration().toMillis() + " мс";
        }
        if (e instanceof WebClientRequestException && e.getCause() instanceof UnknownHostException) {
            return "Supplier Service не найден в сети (контейнер остановлен?)";
        }
        if (e instanceof WebClientRequestException) {
            return "нет соединения с Supplier Service (" + NestedExceptionUtils.getMostSpecificCause(e).getMessage() + ")";
        }
        if (e instanceof WebClientResponseException response) {
            return "Supplier Service ответил " + response.getStatusCode().value();
        }
        return e.toString();
    }
}
