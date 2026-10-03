package baas.supplier.web;

import baas.supplier.repository.ProductRepository;
import baas.supplier.web.Dto.ProductResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/products")
public class ProductController {

    private static final Logger log = LoggerFactory.getLogger(ProductController.class);

    private final ProductRepository products;

    public ProductController(ProductRepository products) {
        this.products = products;
    }

    /**
     * GET /api/products — весь каталог.
     * GET /api/products?ids=1,5 — выбранные товары: так Order Service проверяет наличие
     * перед подтверждением заказа. Товаров, которых нет в каталоге, в ответе не будет.
     */
    @GetMapping
    public List<ProductResponse> findAll(@RequestParam(required = false) List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return products.findAllByOrderById().stream().map(ProductResponse::from).toList();
        }
        List<ProductResponse> found = products.findByIdInOrderById(ids).stream().map(ProductResponse::from).toList();
        log.info("Проверка наличия товаров {}: {}", ids, found.stream()
                .map(p -> "«" + p.name() + "» — " + p.stock() + " шт.")
                .collect(Collectors.joining(", ")));
        return found;
    }

    @GetMapping("/{id}")
    public ProductResponse findById(@PathVariable long id) {
        return products.findWithSupplierById(id)
                .map(ProductResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Товар " + id + " не найден"));
    }
}
