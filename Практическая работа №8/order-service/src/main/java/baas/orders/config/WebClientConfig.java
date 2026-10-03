package baas.orders.config;

import io.netty.channel.ChannelOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

@Configuration
public class WebClientConfig {

    /**
     * Неблокирующий WebClient для Supplier Service: таймаут подключения + передача X-Request-ID.
     * Адрес из DNS Docker кэшируется не дольше 10 с (Docker отдаёт TTL 600 с): после
     * перезапуска контейнера с новым IP клиент быстро найдёт новый адрес.
     */
    @Bean
    public WebClient supplierWebClient(WebClient.Builder builder, SupplierServiceProperties props) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) props.connectTimeout().toMillis())
                .resolver(dns -> dns.cacheMaxTimeToLive(Duration.ofSeconds(10)).queryTimeout(Duration.ofSeconds(1)));

        return builder
                .baseUrl(props.url().toString())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .filter(RequestId.propagate())
                .build();
    }
}
