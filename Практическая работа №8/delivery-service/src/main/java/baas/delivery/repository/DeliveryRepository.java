package baas.delivery.repository;

import baas.delivery.domain.Delivery;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DeliveryRepository extends JpaRepository<Delivery, Long> {

    Optional<Delivery> findByOrderId(Long orderId);

    List<Delivery> findAllByOrderByIdDesc();

    /** Доставки, у которых подошло время следующего шага курьера. */
    List<Delivery> findTop20ByNextStepAtLessThanEqualOrderByNextStepAt(LocalDateTime now);
}
