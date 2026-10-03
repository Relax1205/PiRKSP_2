package baas.gateway.health;

import baas.gateway.config.DnsCacheConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

/**
 * Компоненты orderService, supplierService, deliveryService в health API Gateway.
 * Собраны в группу system: GET /actuator/health/system показывает всю систему разом.
 * В группу readiness они не входят: остановка одного сервиса не делает Gateway «нездоровым».
 */
@Configuration
public class SystemHealthConfig {

    private final ReactorClientHttpConnector connector =
            new ReactorClientHttpConnector(DnsCacheConfig.withShortDnsCache(HttpClient.create()));

    @Bean
    public ReactiveHealthIndicator orderServiceHealthIndicator(WebClient.Builder builder,
                                                               @Value("${services.order-service}") String url) {
        return new DownstreamHealthIndicator(builder.clientConnector(connector), url);
    }

    @Bean
    public ReactiveHealthIndicator supplierServiceHealthIndicator(WebClient.Builder builder,
                                                                  @Value("${services.supplier-service}") String url) {
        return new DownstreamHealthIndicator(builder.clientConnector(connector), url);
    }

    @Bean
    public ReactiveHealthIndicator deliveryServiceHealthIndicator(WebClient.Builder builder,
                                                                  @Value("${services.delivery-service}") String url) {
        return new DownstreamHealthIndicator(builder.clientConnector(connector), url);
    }
}
