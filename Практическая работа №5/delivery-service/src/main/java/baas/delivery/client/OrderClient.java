package baas.delivery.client;

import baas.delivery.domain.OrderStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** HTTP-клиент Order Service на базе RestClient. */
@Component
public class OrderClient {

    private static final ParameterizedTypeReference<List<OrderView>> ORDER_LIST = new ParameterizedTypeReference<>() {
    };

    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    public OrderClient(RestClient orderServiceRestClient, ObjectMapper objectMapper) {
        this.restClient = orderServiceRestClient;
        this.objectMapper = objectMapper;
    }

    public OrderView getOrder(long orderId) {
        return restClient.get()
                .uri("/api/orders/{id}", orderId)
                .retrieve()
                .onStatus(HttpStatusCode::isError, (request, response) -> translateError(response))
                .body(OrderView.class);
    }

    public List<OrderView> getOrders(List<OrderStatus> statuses) {
        return restClient.get()
                .uri(builder -> builder.path("/api/orders").queryParam("status", statuses.toArray()).build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, (request, response) -> translateError(response))
                .body(ORDER_LIST);
    }

    public OrderView changeStatus(long orderId, OrderStatus status) {
        return restClient.patch()
                .uri("/api/orders/{id}/status", orderId)
                .body(Map.of("status", status))
                .retrieve()
                .onStatus(HttpStatusCode::isError, (request, response) -> translateError(response))
                .body(OrderView.class);
    }

    /**
     * 4xx от Order Service (404 — нет заказа, 409 — недопустимый переход статуса)
     * пробрасываются клиенту с тем же кодом, 5xx превращаются в 502 Bad Gateway.
     */
    private void translateError(ClientHttpResponse response) throws IOException {
        HttpStatusCode status = response.getStatusCode();
        String detail = readDetail(response);
        if (status.is4xxClientError()) {
            throw new ResponseStatusException(status, detail);
        }
        throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Order Service ответил " + status.value() + ": " + detail);
    }

    private String readDetail(ClientHttpResponse response) {
        try {
            JsonNode body = objectMapper.readTree(response.getBody());
            if (body != null && body.hasNonNull("detail")) {
                return body.get("detail").asText();
            }
        } catch (IOException | RuntimeException ignored) {
            // тело не JSON — вернём общее сообщение
        }
        return "ошибка Order Service";
    }
}
