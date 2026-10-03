package baas.delivery.config;

import baas.delivery.domain.Delivery;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.repository.DeliveryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Демо-доставки для заказов 2, 3 и 4 из Order Service (данные совпадают с его демо-заказами). */
@Component
public class DemoDataLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoDataLoader.class);

    private final DeliveryRepository repository;

    public DemoDataLoader(DeliveryRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(ApplicationArguments args) {
        LocalDateTime now = LocalDateTime.now();
        Flux.just(
                        new Delivery(null, 2L, "Петрова Анна", "г. Москва, Ленинский проспект, д. 30к2, кв. 117",
                                "Кузнецов Дмитрий", BigDecimal.ZERO, DeliveryStatus.ACCEPTED,
                                now.minusMinutes(20), now.minusMinutes(10)),
                        new Delivery(null, 3L, "Сидоров Пётр", "г. Санкт-Петербург, Садовая, д. 5, кв. 3",
                                "Смирнов Алексей", new BigDecimal("199"), DeliveryStatus.IN_TRANSIT,
                                now.minusMinutes(35), now.minusMinutes(5)),
                        new Delivery(null, 4L, "Иванов Иван", "г. Москва, Тверская, д. 12, кв. 45",
                                "Кузнецов Дмитрий", new BigDecimal("199"), DeliveryStatus.DELIVERED,
                                now.minusHours(3), now.minusHours(2)))
                .concatMap(repository::save)
                .count()
                .subscribe(count -> log.info("Загружено демо-доставок: {}", count));
    }
}
