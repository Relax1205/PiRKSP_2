package baas.orders.repository;

import baas.orders.domain.Order;
import baas.orders.domain.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;

public interface OrderRepository extends JpaRepository<Order, Long> {

    List<Order> findByStatusInOrderById(Collection<OrderStatus> statuses);

    List<Order> findAllByOrderById();
}
