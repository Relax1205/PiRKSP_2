package baas.delivery.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.ResourceAccessException;

@RestControllerAdvice
public class ErrorHandler {

    /** Order Service не отвечает (остановлен, таймаут) — 503 вместо 500. */
    @ExceptionHandler(ResourceAccessException.class)
    public ProblemDetail orderServiceUnavailable(ResourceAccessException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Order Service недоступен: " + e.getMostSpecificCause().getMessage());
        problem.setTitle("Order Service unavailable");
        return problem;
    }
}
