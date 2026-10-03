package baas.orders.repository;

import baas.orders.domain.Order;
import baas.orders.domain.OrderStatus;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

/** Реактивный репозиторий Spring Data R2DBC: методы возвращают Mono/Flux. */
public interface OrderRepository extends ReactiveCrudRepository<Order, Long> {

    Flux<Order> findAllByOrderByIdDesc();

    Flux<Order> findByStatusOrderByIdDesc(OrderStatus status);
}
