package baas.orders.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

/**
 * Маршруты Order Service в функциональном стиле WebFlux (RouterFunction).
 * Delivery Service использует второй стиль — аннотированные контроллеры.
 */
@Configuration
public class OrderRoutes {

    @Bean
    public RouterFunction<ServerResponse> orderRouter(OrderHandler handler) {
        return route()
                .path("/api/orders", builder -> builder
                        .GET("", handler::findAll)
                        .GET("/{id}", handler::findById))
                .build();
    }
}
