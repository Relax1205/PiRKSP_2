package baas.orders.messaging;

import baas.orders.config.RequestId;
import baas.orders.domain.OutboxEvent;
import baas.orders.repository.OutboxRepository;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Отправляет события из outbox в Kafka: раз в pollInterval берёт неотправленные записи
 * по порядку, отправляет и отмечает sentAt. Если Kafka недоступна, события остаются
 * в таблице и уходят позже. Если сервис упадёт между отправкой и отметкой, событие
 * уйдёт повторно — поэтому потребители идемпотентны (at-least-once).
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Duration pollInterval;
    private Disposable loop;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafkaTemplate,
                       @Value("${app.outbox.poll-interval}") Duration pollInterval) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
        this.pollInterval = pollInterval;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        loop = Flux.interval(pollInterval)
                .onBackpressureDrop()
                .concatMap(tick -> publishPending(), 1)
                .subscribe();
    }

    /** Пачка неотправленных событий; при ошибке Kafka остаток пачки ждёт следующего такта. */
    private Mono<Void> publishPending() {
        return outbox.findTop50BySentAtIsNullOrderById()
                .concatMap(this::send)
                .then()
                .onErrorResume(e -> {
                    log.warn("Kafka недоступна: события остаются в outbox и будут отправлены позже ({})",
                            e instanceof TimeoutException ? "брокер не ответил за 15 с" : e.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<OutboxEvent> send(OutboxEvent event) {
        return Mono.fromFuture(() -> kafkaTemplate.send(toRecord(event)))
                // send() может ждать метаданные Kafka до max.block.ms — не на event loop
                .subscribeOn(Schedulers.boundedElastic())
                // чуть больше delivery.timeout.ms (10 с): сначала свою попытку завершит сам producer
                .timeout(Duration.ofSeconds(15))
                .doOnNext(result -> log.info("→ Kafka {}[{}]@{}: {} заказа {} (eventId {})",
                        result.getRecordMetadata().topic(), result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(), event.eventType(), event.orderId(), event.eventId()))
                .flatMap(result -> outbox.save(event.markSent()))
                .contextWrite(ctx -> RequestId.write(ctx, event.requestId()));
    }

    /** Ключ — id заказа (порядок событий заказа сохраняется); X-Request-ID — в заголовке сообщения. */
    private static ProducerRecord<String, String> toRecord(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.topic(), event.messageKey(), event.payload());
        record.headers().add("eventType", event.eventType().getBytes(StandardCharsets.UTF_8));
        if (event.requestId() != null) {
            record.headers().add(RequestId.HEADER, event.requestId().getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @PreDestroy
    public void stop() {
        if (loop != null) {
            loop.dispose();
        }
    }
}
