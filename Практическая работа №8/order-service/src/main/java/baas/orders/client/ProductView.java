package baas.orders.client;

import java.math.BigDecimal;

/**
 * Товар в том виде, в котором его отдаёт Supplier Service (GET /api/products?ids=...).
 * Описан только нужный Order Service набор полей — остальные Jackson пропускает.
 */
public record ProductView(Long id, String name, BigDecimal price, int stock, SupplierRef supplier) {

    public record SupplierRef(Long id, String name) {
    }
}
