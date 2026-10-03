package baas.gateway.health;

import baas.gateway.config.Failures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Состояние одного сервиса для /actuator/health/system: API Gateway запрашивает его
 * /actuator/health (не дольше 3 с) и показывает статус, время ответа и ключевые
 * компоненты — БД, Kafka, circuit breaker.
 */
public class DownstreamHealthIndicator implements ReactiveHealthIndicator {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final List<String> COMPONENTS = List.of("db", "r2dbc", "kafka", "circuitBreakers");

    private final String url;
    private final WebClient webClient;

    public DownstreamHealthIndicator(WebClient.Builder builder, String url) {
        this.url = url;
        this.webClient = builder.baseUrl(url).build();
    }

    @Override
    public Mono<Health> health() {
        return Mono.defer(() -> {
            long started = System.nanoTime();
            return webClient.get()
                    .uri("/actuator/health")
                    .exchangeToMono(response -> response.bodyToMono(JsonNode.class)
                            .defaultIfEmpty(MissingNode.getInstance())
                            .map(body -> toHealth(body, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))))
                    .timeout(TIMEOUT)
                    .onErrorResume(e -> Mono.just(Health.down()
                            .withDetail("url", url)
                            .withDetail("error", describe(e))
                            .build()));
        });
    }

    private Health toHealth(JsonNode body, long responseTimeMs) {
        Health.Builder health = Health.status(body.path("status").asText("UNKNOWN"))
                .withDetail("url", url)
                .withDetail("responseTimeMs", responseTimeMs);
        JsonNode components = body.path("components");
        for (String name : COMPONENTS) {
            JsonNode component = components.path(name);
            if (!component.isMissingNode()) {
                health.withDetail(name, summary(name, component));
            }
        }
        return health.build();
    }

    /** Для circuitBreakers — состояние каждого: "supplierService: OPEN". */
    private static String summary(String name, JsonNode component) {
        String status = component.path("status").asText();
        if (!"circuitBreakers".equals(name)) {
            return status;
        }
        List<String> states = new ArrayList<>();
        for (Map.Entry<String, JsonNode> breaker : component.path("details").properties()) {
            JsonNode details = breaker.getValue();
            states.add(breaker.getKey() + ": " + details.path("details").path("state").asText(details.path("status").asText()));
        }
        return states.isEmpty() ? status : String.join(", ", states);
    }

    private static String describe(Throwable e) {
        if (e instanceof TimeoutException) {
            return "нет ответа за " + TIMEOUT.toSeconds() + " с";
        }
        return Failures.describe(e);
    }
}
