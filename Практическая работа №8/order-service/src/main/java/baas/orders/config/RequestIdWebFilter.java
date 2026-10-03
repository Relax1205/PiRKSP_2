package baas.orders.config;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/** Берёт X-Request-ID из запроса (его ставит API Gateway), кладёт в Reactor Context и в ответ. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdWebFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String requestId = RequestId.normalize(exchange.getRequest().getHeaders().getFirst(RequestId.HEADER));
        exchange.getResponse().getHeaders().set(RequestId.HEADER, requestId);
        return chain.filter(exchange)
                .contextWrite(ctx -> ctx.put(RequestId.CONTEXT_KEY, requestId));
    }
}
