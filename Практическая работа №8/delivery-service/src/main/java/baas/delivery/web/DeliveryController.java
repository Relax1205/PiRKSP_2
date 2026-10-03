package baas.delivery.web;

import baas.delivery.domain.Delivery;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.service.DeliveryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Доставки создаются только по событиям из Kafka, поэтому API — только чтение. */
@RestController
@RequestMapping("/api/deliveries")
public class DeliveryController {

    public record DeliveryResponse(Long id,
                                   Long orderId,
                                   DeliveryStatus status,
                                   String recipient,
                                   String address,
                                   String comment,
                                   BigDecimal price,
                                   String courier,
                                   LocalDateTime createdAt,
                                   LocalDateTime updatedAt,
                                   LocalDateTime nextStepAt) {
        static DeliveryResponse from(Delivery d) {
            return new DeliveryResponse(d.getId(), d.getOrderId(), d.getStatus(), d.getRecipient(), d.getAddress(),
                    d.getComment(), d.getPrice(), d.getCourier(), d.getCreatedAt(), d.getUpdatedAt(), d.getNextStepAt());
        }
    }

    private final DeliveryService deliveryService;

    public DeliveryController(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    /** GET /api/deliveries — все доставки; ?orderId=7 — доставка конкретного заказа. */
    @GetMapping
    public List<DeliveryResponse> findAll(@RequestParam(required = false) Long orderId) {
        return deliveryService.findAll(orderId).stream().map(DeliveryResponse::from).toList();
    }

    @GetMapping("/{id}")
    public DeliveryResponse findById(@PathVariable long id) {
        return DeliveryResponse.from(deliveryService.findById(id));
    }
}
