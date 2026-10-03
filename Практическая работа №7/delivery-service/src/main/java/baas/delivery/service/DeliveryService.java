package baas.delivery.service;

import baas.delivery.client.OrderClient;
import baas.delivery.client.OrderView;
import baas.delivery.config.DeliveryProperties;
import baas.delivery.domain.Delivery;
import baas.delivery.domain.DeliveryEvent;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.error.Errors;
import baas.delivery.repository.DeliveryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Вся логика — реактивные цепочки, без block(). */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryRepository repository;
    private final OrderClient orderClient;
    private final DeliveryEventBus eventBus;
    private final DeliveryProperties props;

    public DeliveryService(DeliveryRepository repository, OrderClient orderClient,
                           DeliveryEventBus eventBus, DeliveryProperties props) {
        this.repository = repository;
        this.orderClient = orderClient;
        this.eventBus = eventBus;
        this.props = props;
    }

    public Flux<Delivery> findAll(DeliveryStatus status) {
        return repository.findAll()
                .filter(d -> status == null || d.status() == status);
    }

    public Mono<Delivery> findById(long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(() -> Errors.deliveryNotFound(id)));
    }

    /** Доставка + заказ из Order Service. Сбой Order Service не ломает ответ. */
    public Mono<DeliveryDetails> details(long id) {
        return findById(id)
                .flatMap(delivery -> orderClient.getOrder(delivery.orderId())
                        .map(order -> DeliveryDetails.withOrder(delivery, order))
                        .onErrorResume(e -> {
                            log.warn("Заказ {} не получен: {}", delivery.orderId(), reason(e));
                            return Mono.just(DeliveryDetails.withoutOrder(delivery, reason(e)));
                        }));
    }

    /**
     * Создание доставки: проверяем, что на заказ ещё нет доставки, берём заказ
     * из Order Service (WebClient), считаем стоимость и сохраняем.
     */
    public Mono<Delivery> create(long orderId) {
        return repository.findByOrderId(orderId)
                .flatMap(existing -> Mono.<OrderView>error(Errors.conflict(
                        "Для заказа " + orderId + " уже есть доставка " + existing.id())))
                .switchIfEmpty(Mono.defer(() -> orderClient.getOrder(orderId)))
                .filter(order -> !order.isCanceled())
                .switchIfEmpty(Mono.error(() -> Errors.conflict("Заказ " + orderId + " отменён, доставка невозможна")))
                .map(this::newDelivery)
                .flatMap(repository::save)
                .doOnNext(this::publish);
    }

    public Mono<Delivery> update(long id, String courier, BigDecimal price, DeliveryStatus status) {
        return findById(id)
                .flatMap(current -> checkUpdate(current, courier, status)
                        .then(repository.save(current.update(courier, price, status)))
                        .doOnNext(updated -> {
                            if (updated.status() != current.status()) {
                                publish(updated);
                            }
                        }));
    }

    public Mono<Void> delete(long id) {
        return repository.deleteById(id)
                .switchIfEmpty(Mono.error(() -> Errors.deliveryNotFound(id)))
                .doOnNext(deleted -> {
                    log.info("Доставка {} удалена", id);
                    eventBus.publish(DeliveryEvent.deleted(deleted));
                })
                .then();
    }

    /**
     * Поток изменений доставки: сначала текущее состояние, затем каждое новое
     * событие из шины. Поток завершается на DELIVERED, CANCELLED или удалении.
     */
    public Flux<DeliveryEvent> watch(long id) {
        return findById(id)
                .flatMapMany(current -> {
                    DeliveryEvent snapshot = DeliveryEvent.of(current);
                    if (snapshot.isLast()) {
                        return Flux.just(snapshot);
                    }
                    return eventBus.events()
                            .filter(event -> event.deliveryId() == id)
                            .takeUntil(DeliveryEvent::isLast)
                            .startWith(snapshot);
                })
                .doOnSubscribe(s -> log.info("SSE: подписка на доставку {}", id))
                .doOnCancel(() -> log.info("SSE: клиент отключился от доставки {}", id))
                .doOnComplete(() -> log.info("SSE: поток доставки {} завершён", id));
    }

    /**
     * Имитация курьера: статусы меняются по одному с паузой.
     * Запрос сразу получает 202, а смена статусов идёт в фоне.
     */
    public Mono<Delivery> simulate(long id) {
        return findById(id)
                .filter(d -> !d.status().isFinal())
                .switchIfEmpty(Mono.error(() -> Errors.conflict("Доставка " + id + " уже завершена")))
                .doOnNext(start -> courierRoute(start).subscribe(
                        d -> log.info("Курьер: доставка {} -> {}", d.id(), d.status()),
                        e -> log.warn("Курьер: имитация доставки {} прервана: {}", id, reason(e))));
    }

    private Flux<Delivery> courierRoute(Delivery start) {
        String courier = start.courier() != null ? start.courier() : props.simulation().defaultCourier();
        List<DeliveryStatus> route = switch (start.status()) {
            case CREATED -> List.of(DeliveryStatus.ACCEPTED, DeliveryStatus.IN_TRANSIT, DeliveryStatus.DELIVERED);
            case ACCEPTED -> List.of(DeliveryStatus.IN_TRANSIT, DeliveryStatus.DELIVERED);
            case IN_TRANSIT -> List.of(DeliveryStatus.DELIVERED);
            case DELIVERED, CANCELLED -> List.of();
        };
        return Flux.fromIterable(route)
                .delayElements(props.simulation().step())
                .concatMap(next -> findById(start.id())
                        .flatMap(current -> update(current.id(), courier, current.price(), next)));
    }

    private Mono<Void> checkUpdate(Delivery current, String courier, DeliveryStatus next) {
        if (current.status().isFinal()) {
            return Mono.error(Errors.conflict(
                    "Доставка " + current.id() + " уже в статусе " + current.status() + ", изменения невозможны"));
        }
        if (next != current.status() && !current.status().canMoveTo(next)) {
            return Mono.error(Errors.conflict(
                    "Недопустимый переход " + current.status() + " -> " + next
                            + ". Разрешены: " + current.status().nextStatuses()));
        }
        if (next.requiresCourier() && (courier == null || courier.isBlank())) {
            return Mono.error(Errors.badRequest("Для статуса " + next + " нужно указать курьера"));
        }
        return Mono.empty();
    }

    private Delivery newDelivery(OrderView order) {
        BigDecimal total = order.total() == null ? BigDecimal.ZERO : order.total();
        BigDecimal price = total.compareTo(props.freeThreshold()) >= 0 ? BigDecimal.ZERO : props.baseCost();
        LocalDateTime now = LocalDateTime.now();
        return new Delivery(null, order.id(), order.recipient(), order.deliveryAddress().line(),
                null, price, DeliveryStatus.CREATED, now, now);
    }

    private void publish(Delivery delivery) {
        log.info("Событие: доставка {} -> {}", delivery.id(), delivery.status());
        eventBus.publish(DeliveryEvent.of(delivery));
    }

    private static String reason(Throwable e) {
        return e instanceof ResponseStatusException rse ? rse.getReason() : e.getMessage();
    }
}
