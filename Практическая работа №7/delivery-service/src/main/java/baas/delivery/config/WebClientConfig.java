package baas.delivery.config;

import io.netty.channel.ChannelOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration
public class WebClientConfig {

    /** WebClient на неблокирующем HTTP-клиенте Reactor Netty с таймаутами. */
    @Bean
    public WebClient orderServiceWebClient(WebClient.Builder builder, OrderServiceProperties props) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) props.connectTimeout().toMillis())
                .responseTimeout(props.responseTimeout());

        return builder
                .baseUrl(props.url().toString())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
