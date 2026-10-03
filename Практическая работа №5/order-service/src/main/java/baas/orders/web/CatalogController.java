package baas.orders.web;

import baas.orders.domain.Customer;
import baas.orders.domain.Product;
import baas.orders.repository.CustomerRepository;
import baas.orders.repository.ProductRepository;
import baas.orders.web.Dto.CustomerRequest;
import baas.orders.web.Dto.CustomerResponse;
import baas.orders.web.Dto.ProductRequest;
import baas.orders.web.Dto.ProductResponse;
import jakarta.validation.Valid;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.List;

/** Справочники Order Service: клиенты (/api/customers) и продукты (/api/products). */
@RestController
public class CatalogController {

    private static final Sort BY_ID = Sort.by("id");

    private final CustomerRepository customers;
    private final ProductRepository products;

    public CatalogController(CustomerRepository customers, ProductRepository products) {
        this.customers = customers;
        this.products = products;
    }

    @GetMapping("/api/customers")
    public List<CustomerResponse> customers() {
        return customers.findAll(BY_ID).stream().map(CustomerResponse::from).toList();
    }

    @GetMapping("/api/customers/{id}")
    public CustomerResponse customer(@PathVariable long id) {
        return customers.findById(id).map(CustomerResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Клиент " + id + " не найден"));
    }

    @PostMapping("/api/customers")
    public ResponseEntity<CustomerResponse> createCustomer(@Valid @RequestBody CustomerRequest request) {
        Customer saved = customers.save(new Customer(request.personalData(), request.address()));
        return ResponseEntity.created(URI.create("/api/customers/" + saved.getId()))
                .body(CustomerResponse.from(saved));
    }

    @GetMapping("/api/products")
    public List<ProductResponse> products() {
        return products.findAll(BY_ID).stream().map(ProductResponse::from).toList();
    }

    @GetMapping("/api/products/{id}")
    public ProductResponse product(@PathVariable long id) {
        return products.findById(id).map(ProductResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Продукт " + id + " не найден"));
    }

    @PostMapping("/api/products")
    public ResponseEntity<ProductResponse> createProduct(@Valid @RequestBody ProductRequest request) {
        Product saved = products.save(new Product(request.name(), request.description(), request.price()));
        return ResponseEntity.created(URI.create("/api/products/" + saved.getId()))
                .body(ProductResponse.from(saved));
    }
}
