package baas.delivery.web;

import baas.delivery.config.InstanceInfo;
import baas.delivery.service.DeliveryInfo;
import baas.delivery.service.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/deliveries")
public class DeliveryController {

    private static final Logger log = LoggerFactory.getLogger(DeliveryController.class);

    /** Любой ответ сервиса содержит экземпляр, который его обработал. */
    public record Response<T>(InstanceInfo.View servedBy, T data) {
    }

    private final DeliveryService deliveryService;
    private final InstanceInfo instance;

    public DeliveryController(DeliveryService deliveryService, InstanceInfo instance) {
        this.deliveryService = deliveryService;
        this.instance = instance;
    }

    /** Для демонстрации балансировки: какой экземпляр ответил. */
    @GetMapping("/instance")
    public InstanceInfo.View instance() {
        log.info("Запрос обслужен экземпляром {}", instance.view().hostname());
        return instance.view();
    }

    @GetMapping
    public Response<List<DeliveryInfo>> activeDeliveries() {
        return wrap(deliveryService.getActiveDeliveries());
    }

    @GetMapping("/{orderId}")
    public Response<DeliveryInfo> delivery(@PathVariable long orderId) {
        return wrap(deliveryService.getDelivery(orderId));
    }

    @PostMapping("/{orderId}/dispatch")
    public Response<DeliveryInfo> dispatch(@PathVariable long orderId) {
        return wrap(deliveryService.dispatch(orderId));
    }

    @PostMapping("/{orderId}/complete")
    public Response<DeliveryInfo> complete(@PathVariable long orderId) {
        return wrap(deliveryService.complete(orderId));
    }

    private <T> Response<T> wrap(T data) {
        return new Response<>(instance.view(), data);
    }
}
