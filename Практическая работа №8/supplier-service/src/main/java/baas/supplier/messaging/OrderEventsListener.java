package baas.supplier.messaging;

import baas.supplier.config.RequestId;
import baas.supplier.service.StockService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Потребитель топика order-events (группа supplier-service). То же событие OrderCreated
 * независимо от него читает Delivery Service — у каждой группы свои смещения.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final ObjectMapper objectMapper;
    private final StockService stockService;

    public OrderEventsListener(ObjectMapper objectMapper, StockService stockService) {
        this.objectMapper = objectMapper;
        this.stockService = stockService;
    }

    @KafkaListener(topics = "${app.kafka.order-events-topic}")
    public void onOrderEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {
        RequestId.putMdc(RequestId.fromKafka(record.headers()));
        try {
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
            if (!OrderCreatedEvent.TYPE.equals(event.type())) {
                return;
            }
            log.info("← Kafka {}[{}]@{}: OrderCreated заказа {} (eventId {})",
                    record.topic(), record.partition(), record.offset(), event.orderId(), event.eventId());
            stockService.writeOff(event);
        } finally {
            RequestId.clearMdc();
        }
    }
}
