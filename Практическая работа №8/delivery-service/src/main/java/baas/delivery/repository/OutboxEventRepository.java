package baas.delivery.repository;

import baas.delivery.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /** Ещё не отправленные события в порядке записи. */
    List<OutboxEvent> findTop50BySentAtIsNullOrderById();
}
