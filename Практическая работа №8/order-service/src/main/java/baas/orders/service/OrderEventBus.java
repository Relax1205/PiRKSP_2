package baas.orders.service;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

/**
 * Шина изменений статусов заказов (как DeliveryEventBus в ПР №7). Каждое изменение,
 * уже записанное в БД, публикуется в Sink, а SSE-подписчики получают его из общего
 * горячего Flux.
 */
@Component
public class OrderEventBus {

    // multicast: одно событие получают все текущие подписчики;
    // directBestEffort: если подписчиков нет, событие никому не уходит (история есть в БД)
    private final Sinks.Many<OrderEvent> sink = Sinks.many().multicast().directBestEffort();

    public void publish(OrderEvent event) {
        // события приходят из разных потоков (HTTP, Kafka) — при гонке emitNext повторяется
        sink.emitNext(event, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
    }

    public Flux<OrderEvent> events() {
        return sink.asFlux();
    }
}
