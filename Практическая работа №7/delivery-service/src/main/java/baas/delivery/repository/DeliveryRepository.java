package baas.delivery.repository;

import baas.delivery.domain.Delivery;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Хранилище доставок в памяти с реактивным интерфейсом.
 * Отсутствие записи — пустой Mono, а не null: дальше это обрабатывает switchIfEmpty.
 */
@Repository
public class DeliveryRepository {

    private final Map<Long, Delivery> storage = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    public Flux<Delivery> findAll() {
        return Flux.defer(() -> Flux.fromStream(storage.values().stream()
                .sorted(Comparator.comparing(Delivery::id))));
    }

    public Mono<Delivery> findById(long id) {
        return Mono.fromSupplier(() -> storage.get(id));
    }

    public Mono<Delivery> findByOrderId(long orderId) {
        return findAll().filter(d -> d.orderId() == orderId).next();
    }

    public Mono<Delivery> save(Delivery delivery) {
        return Mono.fromSupplier(() -> {
            Delivery toSave = delivery.id() == null ? delivery.withId(sequence.incrementAndGet()) : delivery;
            storage.put(toSave.id(), toSave);
            return toSave;
        });
    }

    /** Удалённая доставка или пустой Mono, если её не было. */
    public Mono<Delivery> deleteById(long id) {
        return Mono.fromSupplier(() -> storage.remove(id));
    }
}
