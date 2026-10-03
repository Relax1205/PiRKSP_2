package baas.orders.messaging;

import baas.orders.config.RequestId;
import baas.orders.service.OrderService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Потребитель топика delivery-events (группа order-service): статусы доставки → статус заказа. */
@Component
public class DeliveryEventsListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventsListener.class);

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public DeliveryEventsListener(ObjectMapper objectMapper, OrderService orderService) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.kafka.delivery-events-topic}")
    public void onDeliveryEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String requestId = RequestId.fromKafka(record.headers());
        RequestId.putMdc(requestId);
        try {
            DeliveryEvent event = objectMapper.readValue(record.value(), DeliveryEvent.class);
            log.info("← Kafka {}[{}]@{}: {} — доставка {} заказа {}, статус {}", record.topic(), record.partition(),
                    record.offset(), event.type(), event.deliveryId(), event.orderId(), event.status());
            // Поток Kafka-консьюмера — не event loop, здесь можно дождаться конца реактивной цепочки.
            // Смещение в Kafka фиксируется только после того, как статус заказа записан в БД.
            orderService.applyDeliveryEvent(event)
                    .contextWrite(ctx -> RequestId.write(ctx, requestId))
                    .block(Duration.ofSeconds(10));
        } finally {
            RequestId.clearMdc();
        }
    }
}
