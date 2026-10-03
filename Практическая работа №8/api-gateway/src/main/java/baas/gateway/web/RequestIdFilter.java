package baas.gateway.web;

import baas.gateway.config.RequestId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * Глобальный фильтр всех маршрутов: X-Request-ID берётся из запроса клиента или
 * генерируется, добавляется в запрос к сервису и в ответ клиенту, пишется в лог
 * вместе с маршрутом, кодом ответа и временем обработки.
 */
@Component
public class RequestIdFilter implements GlobalFilter, Ordered {

    /** Исходный путь запроса — для fallback, куда запрос попадает уже с путём /fallback/... */
    public static final String ORIGINAL_PATH_ATTR = RequestIdFilter.class.getName() + ".originalPath";

    private static final Logger log = LoggerFactory.getLogger(RequestIdFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String requestId = RequestId.normalize(exchange.getRequest().getHeaders().getFirst(RequestId.HEADER));
        ServerHttpRequest request = exchange.getRequest().mutate()
                .headers(headers -> headers.set(RequestId.HEADER, requestId))
                .build();
        exchange.getAttributes().put(ORIGINAL_PATH_ATTR, request.getURI().getRawPath());
        // Сервис сам вернёт X-Request-ID; если ответа сервиса нет (fallback) — добавим его здесь
        exchange.getResponse().beforeCommit(() -> {
            HttpHeaders headers = exchange.getResponse().getHeaders();
            if (!headers.containsKey(RequestId.HEADER)) {
                headers.set(RequestId.HEADER, requestId);
            }
            return Mono.empty();
        });

        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        String target = route == null ? "?" : route.getId() + " → " + route.getUri();
        String query = request.getURI().getRawQuery();
        String call = request.getMethod().name() + " " + request.getURI().getRawPath() + (query == null ? "" : "?" + query);
        long started = System.nanoTime();

        return chain.filter(exchange.mutate().request(request).build())
                .doOnSubscribe(s -> RequestId.withMdc(requestId, () -> log.info("→ {} [маршрут {}]", call, target)))
                .doFinally(signal -> RequestId.withMdc(requestId, () -> log.info("← {} {} за {} мс",
                        exchange.getResponse().getStatusCode(), call,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))))
                .contextWrite(ctx -> ctx.put(RequestId.CONTEXT_KEY, requestId));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
