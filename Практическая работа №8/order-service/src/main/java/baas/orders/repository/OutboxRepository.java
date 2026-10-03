package baas.orders.repository;

import baas.orders.domain.OutboxEvent;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface OutboxRepository extends ReactiveCrudRepository<OutboxEvent, Long> {

    /** Ещё не отправленные события в порядке записи. */
    Flux<OutboxEvent> findTop50BySentAtIsNullOrderById();

    Mono<OutboxEvent> findFirstByOrderIdAndEventType(Long orderId, String eventType);
}
