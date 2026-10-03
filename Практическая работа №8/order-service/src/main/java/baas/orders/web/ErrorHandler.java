package baas.orders.web;

import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;

/**
 * Все ошибки — в формате RFC 7807 Problem Details. Базовый класс уже умеет это для
 * ResponseStatusException и ErrorResponseException (заказ отклонён); здесь — понятные тексты.
 */
@RestControllerAdvice
public class ErrorHandler extends ResponseEntityExceptionHandler {

    /** Не прошла валидация тела запроса (@Valid). */
    @Override
    protected Mono<ResponseEntity<Object>> handleWebExchangeBindException(
            WebExchangeBindException ex, HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        String errors = ex.getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        ex.getBody().setDetail("Ошибка в теле запроса — " + errors);
        return super.handleWebExchangeBindException(ex, headers, status, exchange);
    }

    /** Неверный формат параметра или нечитаемый JSON. */
    @Override
    protected Mono<ResponseEntity<Object>> handleServerWebInputException(
            ServerWebInputException ex, HttpHeaders headers, HttpStatusCode status, ServerWebExchange exchange) {
        if (ex.getCause() instanceof TypeMismatchException mismatch && ex.getMethodParameter() != null) {
            ex.getBody().setDetail("Параметр " + ex.getMethodParameter().getParameterName()
                    + " имеет неверный формат: " + mismatch.getValue());
        } else {
            ex.getBody().setDetail("Некорректное тело запроса: проверьте JSON и значения полей");
        }
        return super.handleServerWebInputException(ex, headers, status, exchange);
    }
}
