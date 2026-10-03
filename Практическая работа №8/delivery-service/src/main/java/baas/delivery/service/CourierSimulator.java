package baas.delivery.service;

import baas.delivery.config.RequestId;
import baas.delivery.domain.Delivery;
import baas.delivery.repository.DeliveryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Имитация работы курьера: раз в секунду находит доставки, у которых подошло время
 * следующего шага (next_step_at), и продвигает их по статусам. Время шага хранится
 * в БД, поэтому после перезапуска сервиса доставки продолжаются с того же места.
 */
@Component
@ConditionalOnProperty(name = "delivery.courier.simulation", havingValue = "true", matchIfMissing = true)
public class CourierSimulator {

    private static final Logger log = LoggerFactory.getLogger(CourierSimulator.class);

    private final DeliveryRepository deliveries;
    private final DeliveryService deliveryService;

    public CourierSimulator(DeliveryRepository deliveries, DeliveryService deliveryService) {
        this.deliveries = deliveries;
        this.deliveryService = deliveryService;
    }

    @Scheduled(fixedDelayString = "${delivery.courier.poll-interval-ms}")
    public void moveCouriers() {
        for (Delivery due : deliveries.findTop20ByNextStepAtLessThanEqualOrderByNextStepAt(LocalDateTime.now())) {
            // requestId исходного запроса: шаги курьера видны в логах под тем же идентификатором
            RequestId.putMdc(due.getRequestId());
            try {
                deliveryService.nextStep(due.getId());
            } catch (ObjectOptimisticLockingFailureException e) {
                log.debug("Доставку {} одновременно изменили — шаг будет повторён", due.getId());
            } catch (RuntimeException e) {
                log.warn("Курьер: не удалось продвинуть доставку {}: {}", due.getId(), e.getMessage());
            } finally {
                RequestId.clearMdc();
            }
        }
    }
}
