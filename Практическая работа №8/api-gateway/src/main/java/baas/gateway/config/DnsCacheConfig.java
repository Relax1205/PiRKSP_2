package baas.gateway.config;

import org.springframework.cloud.gateway.config.HttpClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

/**
 * DNS Docker отдаёт адреса контейнеров с TTL 600 с, и Netty кэширует их на это время.
 * Если контейнер перезапустится с другим IP, Gateway 10 минут ходил бы по старому адресу,
 * поэтому кэш ограничен 10 секундами. Имя остановленного контейнера DNS Docker пересылает
 * внешнему DNS и ждёт до 5 с — поэтому запрос к DNS ограничен 1 секундой.
 */
@Configuration
public class DnsCacheConfig {

    public static final Duration DNS_CACHE_TTL = Duration.ofSeconds(10);

    public static HttpClient withShortDnsCache(HttpClient httpClient) {
        return httpClient.resolver(dns -> dns.cacheMaxTimeToLive(DNS_CACHE_TTL).queryTimeout(Duration.ofSeconds(1)));
    }

    /** HTTP-клиент, через который Spring Cloud Gateway проксирует запросы в сервисы. */
    @Bean
    public HttpClientCustomizer shortDnsCacheCustomizer() {
        return DnsCacheConfig::withShortDnsCache;
    }
}
