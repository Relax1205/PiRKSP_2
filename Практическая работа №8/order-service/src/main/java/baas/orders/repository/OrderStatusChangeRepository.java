package baas.orders.repository;

import baas.orders.domain.OrderStatusChange;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface OrderStatusChangeRepository extends ReactiveCrudRepository<OrderStatusChange, Long> {

    Flux<OrderStatusChange> findByOrderIdOrderById(Long orderId);
}
