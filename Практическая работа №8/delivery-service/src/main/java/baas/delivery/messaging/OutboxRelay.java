package baas.delivery.messaging;

import baas.delivery.config.RequestId;
import baas.delivery.domain.OutboxEvent;
import baas.delivery.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Отправляет события из outbox в Kafka. Если Kafka недоступна, событие остаётся
 * в таблице и уходит при следующей попытке. Если сервис упадёт между отправкой
 * и отметкой sentAt, событие уйдёт повторно — поэтому потребители идемпотентны.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxEventRepository outbox, KafkaTemplate<String, String> kafkaTemplate) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void publishPending() {
        for (OutboxEvent event : outbox.findTop50BySentAtIsNullOrderById()) {
            RequestId.putMdc(event.getRequestId());
            try {
                RecordMetadata sent = kafkaTemplate.send(toRecord(event)).get(10, TimeUnit.SECONDS).getRecordMetadata();
                log.info("→ Kafka {}[{}]@{}: {} (eventId {})",
                        sent.topic(), sent.partition(), sent.offset(), event.getEventType(), event.getEventId());
                event.markSent();
                outbox.save(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Kafka недоступна: {} {} остаётся в outbox и будет отправлено позже ({})",
                        event.getEventType(), event.getEventId(), e.getMessage());
                return;
            } finally {
                RequestId.clearMdc();
            }
        }
    }

    /** X-Request-ID едет в заголовке сообщения: это метаданные, а не данные события. */
    private static ProducerRecord<String, String> toRecord(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.getTopic(), event.getMessageKey(), event.getPayload());
        record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));
        if (event.getRequestId() != null) {
            record.headers().add(RequestId.HEADER, event.getRequestId().getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }
}
