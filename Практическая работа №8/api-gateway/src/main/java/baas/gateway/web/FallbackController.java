package baas.gateway.web;

import baas.gateway.config.Failures;
import baas.gateway.config.RequestId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.URI;

/**
 * Fallback фильтра CircuitBreaker: сервис не отвечает или circuit breaker открыт —
 * клиент сразу получает понятный 503 в формате Problem Details, а не 500 или ожидание.
 */
@RestController
public class FallbackController {

    private static final Logger log = LoggerFactory.getLogger(FallbackController.class);

    @RequestMapping("/fallback/{service}")
    public Mono<ResponseEntity<ProblemDetail>> fallback(@PathVariable String service, ServerWebExchange exchange) {
        Throwable cause = exchange.getAttribute(ServerWebExchangeUtils.CIRCUITBREAKER_EXECUTION_EXCEPTION_ATTR);
        String reason = Failures.describe(cause);
        String requestId = exchange.getRequest().getHeaders().getFirst(RequestId.HEADER);
        String path = exchange.getAttributeOrDefault(RequestIdFilter.ORIGINAL_PATH_ATTR, exchange.getRequest().getPath().value());
        RequestId.withMdc(requestId, () -> log.warn("Fallback: {} недоступен ({}) — ответ 503 на {}", service, reason, path));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Сервис " + service + " временно недоступен: " + reason);
        problem.setTitle("Service Unavailable");
        problem.setInstance(URI.create(path));
        problem.setProperty("service", service);
        problem.setProperty("requestId", requestId);
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problem));
    }
}
