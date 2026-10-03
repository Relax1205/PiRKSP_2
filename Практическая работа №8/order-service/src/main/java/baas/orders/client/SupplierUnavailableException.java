package baas.orders.client;

/** Supplier Service не ответил: таймаут, нет соединения, 5xx или открыт circuit breaker. */
public class SupplierUnavailableException extends RuntimeException {

    public SupplierUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
