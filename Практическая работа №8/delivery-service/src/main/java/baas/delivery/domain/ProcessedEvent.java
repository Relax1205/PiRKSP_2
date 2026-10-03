package baas.delivery.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Обработанное событие Kafka. Первичный ключ — eventId, поэтому одно и то же
 * событие нельзя записать дважды: на этом держится идемпотентность потребителя.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent implements Persistable<UUID> {

    @Id
    private UUID eventId;

    @Column(nullable = false, length = 64)
    private String eventType;

    private Long orderId;

    @Column(nullable = false)
    private LocalDateTime processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(UUID eventId, String eventType, Long orderId) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.processedAt = LocalDateTime.now();
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    /** Всегда INSERT, а не merge: повторная запись того же eventId упрётся в первичный ключ. */
    @Override
    public boolean isNew() {
        return true;
    }

    public String getEventType() {
        return eventType;
    }

    public Long getOrderId() {
        return orderId;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }
}
