package baas.delivery.error;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * Ошибки API. ResponseStatusException WebFlux сам превращает в ответ
 * RFC 7807 Problem Details (spring.webflux.problemdetails.enabled).
 */
public final class Errors {

    private Errors() {
    }

    public static ResponseStatusException deliveryNotFound(long id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Доставка " + id + " не найдена");
    }

    public static ResponseStatusException orderNotFound(long orderId) {
        return new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                "Заказ " + orderId + " не найден в Order Service");
    }

    public static ResponseStatusException conflict(String detail) {
        return new ResponseStatusException(HttpStatus.CONFLICT, detail);
    }

    public static ResponseStatusException badRequest(String detail) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, detail);
    }

    public static ResponseStatusException orderServiceUnavailable(String detail) {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, detail);
    }
}
