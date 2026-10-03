package baas.supplier.web;

import baas.supplier.repository.ProductRepository;
import baas.supplier.repository.SupplierRepository;
import baas.supplier.web.Dto.SupplierResponse;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/suppliers")
public class SupplierController {

    private final SupplierRepository suppliers;
    private final ProductRepository products;

    public SupplierController(SupplierRepository suppliers, ProductRepository products) {
        this.suppliers = suppliers;
        this.products = products;
    }

    /** Поставщики вместе с их товарами. */
    @GetMapping
    @Transactional(readOnly = true)
    public List<SupplierResponse> findAll() {
        return suppliers.findAll(Sort.by("id")).stream()
                .map(s -> SupplierResponse.from(s, products.findBySupplierIdOrderById(s.getId())))
                .toList();
    }

    @GetMapping("/{id}")
    @Transactional(readOnly = true)
    public SupplierResponse findById(@PathVariable long id) {
        return suppliers.findById(id)
                .map(s -> SupplierResponse.from(s, products.findBySupplierIdOrderById(id)))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Поставщик " + id + " не найден"));
    }
}
