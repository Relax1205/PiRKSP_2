package baas.delivery.service;

import baas.delivery.domain.DeliveryEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

/**
 * Шина событий об изменении доставок. Каждое изменение публикуется в Sink,
 * а SSE-подписчики получают его из общего горячего Flux.
 */
@Component
public class DeliveryEventBus {

    // multicast: одно событие получают все текущие подписчики;
    // directBestEffort: если подписчиков нет, событие просто никому не уходит
    private final Sinks.Many<DeliveryEvent> sink = Sinks.many().multicast().directBestEffort();

    public void publish(DeliveryEvent event) {
        // события могут прийти из разных потоков — при гонке emitNext повторяется
        sink.emitNext(event, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
    }

    public Flux<DeliveryEvent> events() {
        return sink.asFlux();
    }
}
