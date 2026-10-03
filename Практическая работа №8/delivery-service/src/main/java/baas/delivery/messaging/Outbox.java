package baas.delivery.messaging;

import baas.delivery.domain.OutboxEvent;
import baas.delivery.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Запись событий в outbox. Вызывается внутри транзакции, которая меняет доставку. */
@Component
public class Outbox {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final String deliveryEventsTopic;

    public Outbox(OutboxEventRepository repository, ObjectMapper objectMapper,
                  @Value("${app.kafka.delivery-events-topic}") String deliveryEventsTopic) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.deliveryEventsTopic = deliveryEventsTopic;
    }

    /** Ключ сообщения — id заказа: все события одного заказа попадут в один раздел и придут по порядку. */
    public void add(DeliveryEvent event, String requestId) {
        repository.save(new OutboxEvent(event.eventId(), event.type(), deliveryEventsTopic,
                String.valueOf(event.orderId()), toJson(event), requestId));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Не удалось сериализовать событие", e);
        }
    }
}
