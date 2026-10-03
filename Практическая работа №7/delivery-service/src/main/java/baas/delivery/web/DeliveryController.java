package baas.delivery.web;

import baas.delivery.domain.Delivery;
import baas.delivery.domain.DeliveryEvent;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.service.DeliveryDetails;
import baas.delivery.service.DeliveryService;
import baas.delivery.web.Dto.CreateDeliveryRequest;
import baas.delivery.web.Dto.UpdateDeliveryRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/api/deliveries")
public class DeliveryController {

    private final DeliveryService deliveryService;

    public DeliveryController(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    /** Несколько объектов — Flux. GET /api/deliveries?status=IN_TRANSIT */
    @GetMapping
    public Flux<Delivery> findAll(@RequestParam(required = false) DeliveryStatus status) {
        return deliveryService.findAll(status);
    }

    /** Один объект — Mono. Аналог RSocket Request-Response. */
    @GetMapping("/{id}")
    public Mono<Delivery> findById(@PathVariable long id) {
        return deliveryService.findById(id);
    }

    @GetMapping("/{id}/details")
    public Mono<DeliveryDetails> details(@PathVariable long id) {
        return deliveryService.details(id);
    }

    @PostMapping
    public Mono<ResponseEntity<Delivery>> create(@Valid @RequestBody CreateDeliveryRequest request) {
        return deliveryService.create(request.orderId())
                .map(created -> ResponseEntity.created(URI.create("/api/deliveries/" + created.id())).body(created));
    }

    @PutMapping("/{id}")
    public Mono<Delivery> update(@PathVariable long id, @Valid @RequestBody UpdateDeliveryRequest request) {
        return deliveryService.update(id, request.courier(), request.price(), request.status());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable long id) {
        return deliveryService.delete(id);
    }

    /**
     * Поток изменений статуса по Server-Sent Events. Аналог RSocket Request-Stream.
     * Если доставки нет, клиент получает событие error, а не обрыв соединения.
     */
    @GetMapping(path = "/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> stream(@PathVariable long id) {
        return deliveryService.watch(id)
                .index()
                .map(indexed -> {
                    DeliveryEvent event = indexed.getT2();
                    return ServerSentEvent.<Object>builder(event)
                            .id(String.valueOf(indexed.getT1() + 1))
                            .event(event.type())
                            .build();
                })
                .onErrorResume(ResponseStatusException.class, e -> Flux.just(ServerSentEvent.<Object>builder()
                        .event("error")
                        .data(Map.of("status", e.getStatusCode().value(), "detail", e.getReason()))
                        .build()));
    }

    /** Запускает имитацию курьера; статусы будут приходить в /stream. */
    @PostMapping("/{id}/simulate")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Delivery> simulate(@PathVariable long id) {
        return deliveryService.simulate(id);
    }
}
