package baas.delivery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;

@Configuration
public class RestClientConfig {

    /** java.net.http.HttpClient — в отличие от HttpURLConnection поддерживает PATCH. */
    @Bean
    public RestClient orderServiceRestClient(RestClient.Builder builder, OrderServiceProperties props) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(props.connectTimeout())
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(props.readTimeout());

        return builder
                .baseUrl(props.url().toString())
                .requestFactory(requestFactory)
                .build();
    }
}
