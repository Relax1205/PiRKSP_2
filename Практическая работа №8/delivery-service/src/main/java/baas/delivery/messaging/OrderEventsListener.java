package baas.delivery.messaging;

import baas.delivery.config.RequestId;
import baas.delivery.service.DeliveryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Потребитель топика order-events (группа delivery-service): по OrderCreated создаётся доставка. */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final ObjectMapper objectMapper;
    private final DeliveryService deliveryService;

    public OrderEventsListener(ObjectMapper objectMapper, DeliveryService deliveryService) {
        this.objectMapper = objectMapper;
        this.deliveryService = deliveryService;
    }

    @KafkaListener(topics = "${app.kafka.order-events-topic}")
    public void onOrderEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String requestId = RequestId.fromKafka(record.headers());
        RequestId.putMdc(requestId);
        try {
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
            if (!OrderCreatedEvent.TYPE.equals(event.type())) {
                return;
            }
            log.info("← Kafka {}[{}]@{}: OrderCreated заказа {} (eventId {})",
                    record.topic(), record.partition(), record.offset(), event.orderId(), event.eventId());
            deliveryService.createFor(event, requestId);
        } finally {
            RequestId.clearMdc();
        }
    }
}
