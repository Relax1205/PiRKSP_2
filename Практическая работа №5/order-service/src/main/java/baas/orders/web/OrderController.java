package baas.orders.web;

import baas.orders.domain.OrderStatus;
import baas.orders.service.OrderService;
import baas.orders.web.Dto.ChangeStatusRequest;
import baas.orders.web.Dto.CreateOrderRequest;
import baas.orders.web.Dto.OrderResponse;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** GET /api/orders?status=ASSEMBLY&status=DELIVERY — фильтр по статусам необязателен. */
    @GetMapping
    public List<OrderResponse> findAll(@RequestParam(name = "status", required = false) List<OrderStatus> statuses) {
        return orderService.findAll(statuses);
    }

    @GetMapping("/{id}")
    public OrderResponse findById(@PathVariable long id) {
        return orderService.findById(id);
    }

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        OrderResponse created = orderService.create(request);
        return ResponseEntity.created(URI.create("/api/orders/" + created.id())).body(created);
    }

    @PatchMapping("/{id}/status")
    public OrderResponse changeStatus(@PathVariable long id, @Valid @RequestBody ChangeStatusRequest request) {
        return orderService.changeStatus(id, request.status());
    }
}
