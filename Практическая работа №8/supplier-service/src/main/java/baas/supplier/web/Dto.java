package baas.supplier.web;

import baas.supplier.domain.Product;
import baas.supplier.domain.Supplier;

import java.math.BigDecimal;
import java.util.List;

/** Ответы REST API Supplier Service. */
public final class Dto {

    private Dto() {
    }

    public record SupplierRef(Long id, String name) {
    }

    public record ProductResponse(Long id,
                                  String name,
                                  String description,
                                  BigDecimal price,
                                  int stock,
                                  boolean available,
                                  SupplierRef supplier) {
        public static ProductResponse from(Product p) {
            return new ProductResponse(p.getId(), p.getName(), p.getDescription(), p.getPrice(),
                    p.getStock(), p.getStock() > 0,
                    new SupplierRef(p.getSupplier().getId(), p.getSupplier().getName()));
        }
    }

    public record SupplierResponse(Long id,
                                   String name,
                                   String inn,
                                   String warehouseAddress,
                                   List<ProductResponse> products) {
        public static SupplierResponse from(Supplier s, List<Product> products) {
            return new SupplierResponse(s.getId(), s.getName(), s.getInn(), s.getWarehouseAddress(),
                    products.stream().map(ProductResponse::from).toList());
        }
    }
}
