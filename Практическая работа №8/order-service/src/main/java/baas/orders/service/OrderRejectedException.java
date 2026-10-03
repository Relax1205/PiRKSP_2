package baas.orders.service;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;

/**
 * Заказ отклонён (REJECTED). Ответ — Problem Details с номером заказа:
 * 409 — товара не хватает, 422 — товара нет в каталоге, 503 — Supplier Service недоступен.
 */
public class OrderRejectedException extends ErrorResponseException {

    public OrderRejectedException(HttpStatus status, long orderId, String reason) {
        super(status, problem(status, orderId, reason), null);
    }

    private static ProblemDetail problem(HttpStatus status, long orderId, String reason) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, reason);
        problem.setTitle("Заказ отклонён");
        problem.setProperty("orderId", orderId);
        problem.setProperty("orderStatus", "REJECTED");
        return problem;
    }
}
