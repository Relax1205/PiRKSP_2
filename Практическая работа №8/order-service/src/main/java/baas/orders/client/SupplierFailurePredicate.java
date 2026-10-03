package baas.orders.client;

import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Что считать сбоем Supplier Service — для Retry (повторять ли) и CircuitBreaker
 * (засчитывать ли как ошибку): таймаут, ошибка соединения, ответ 5xx.
 * Ответ 4xx — не сбой: сервис жив и ответил, повтор ничего не изменит.
 * Указан в application.yml: retry-exception-predicate и record-failure-predicate.
 */
public class SupplierFailurePredicate implements Predicate<Throwable> {

    @Override
    public boolean test(Throwable e) {
        return e instanceof TimeoutException
                || e instanceof WebClientRequestException
                || (e instanceof WebClientResponseException response && response.getStatusCode().is5xxServerError());
    }
}
