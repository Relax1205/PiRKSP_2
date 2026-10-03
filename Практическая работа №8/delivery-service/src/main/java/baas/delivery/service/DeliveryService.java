package baas.delivery.service;

import baas.delivery.config.DeliveryProperties;
import baas.delivery.domain.Delivery;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.domain.ProcessedEvent;
import baas.delivery.messaging.DeliveryEvent;
import baas.delivery.messaging.OrderCreatedEvent;
import baas.delivery.messaging.Outbox;
import baas.delivery.repository.DeliveryRepository;
import baas.delivery.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryRepository deliveries;
    private final ProcessedEventRepository processedEvents;
    private final Outbox outbox;
    private final DeliveryProperties props;

    public DeliveryService(DeliveryRepository deliveries, ProcessedEventRepository processedEvents,
                           Outbox outbox, DeliveryProperties props) {
        this.deliveries = deliveries;
        this.processedEvents = processedEvents;
        this.outbox = outbox;
        this.props = props;
    }

    /**
     * Создание доставки по событию OrderCreated. Идемпотентно, защита в два уровня:
     * 1) eventId уже есть в processed_event — это повтор того же сообщения, пропускаем;
     * 2) на заказ уже есть доставка (уникальный order_id) — дубль не создаём.
     * Доставка, отметка об обработке и событие DeliveryCreated (outbox) — одна транзакция.
     */
    @Transactional
    public void createFor(OrderCreatedEvent event, String requestId) {
        if (processedEvents.existsById(event.eventId())) {
            log.warn("Повторное событие {} (OrderCreated, заказ {}) уже обработано — доставка повторно не создаётся",
                    event.eventId(), event.orderId());
            return;
        }
        Optional<Delivery> existing = deliveries.findByOrderId(event.orderId());
        if (existing.isPresent()) {
            log.warn("Для заказа {} уже есть доставка {} — событие {} дубль не создаёт",
                    event.orderId(), existing.get().getId(), event.eventId());
        } else {
            Delivery delivery = deliveries.save(new Delivery(event.orderId(), event.customerName(),
                    event.deliveryAddress(), event.comment(), price(event.total()), requestId, nextStepAt()));
            outbox.add(DeliveryEvent.of(DeliveryEvent.CREATED, delivery), requestId);
            log.info("Создана доставка {} для заказа {}: {}, {}, стоимость доставки {} ₽",
                    delivery.getId(), delivery.getOrderId(), delivery.getRecipient(), delivery.getAddress(),
                    delivery.getPrice());
        }
        processedEvents.save(new ProcessedEvent(event.eventId(), event.type(), event.orderId()));
    }

    /** Следующий шаг курьера: CREATED → ACCEPTED → IN_TRANSIT → DELIVERED. */
    @Transactional
    public void nextStep(long deliveryId) {
        Delivery delivery = deliveries.findById(deliveryId).orElseThrow(() -> notFound(deliveryId));
        if (delivery.getStatus().isFinal()) {
            return;
        }
        DeliveryStatus previous = delivery.getStatus();
        DeliveryStatus next = previous.nextStep();
        String courier = delivery.getCourier() != null ? delivery.getCourier() : pickCourier(delivery);
        delivery.moveTo(next, courier, next.isFinal() ? null : nextStepAt());
        outbox.add(DeliveryEvent.of(DeliveryEvent.STATUS_CHANGED, delivery), delivery.getRequestId());
        log.info("Доставка {} (заказ {}): {} -> {}, курьер {}",
                delivery.getId(), delivery.getOrderId(), previous, next, courier);
    }

    @Transactional(readOnly = true)
    public List<Delivery> findAll(Long orderId) {
        if (orderId != null) {
            return deliveries.findByOrderId(orderId).stream().toList();
        }
        return deliveries.findAllByOrderByIdDesc();
    }

    @Transactional(readOnly = true)
    public Delivery findById(long id) {
        return deliveries.findById(id).orElseThrow(() -> notFound(id));
    }

    /** Тариф из ПР №5: от суммы freeThreshold доставка бесплатная. */
    private BigDecimal price(BigDecimal orderTotal) {
        BigDecimal total = orderTotal == null ? BigDecimal.ZERO : orderTotal;
        return total.compareTo(props.freeThreshold()) >= 0 ? BigDecimal.ZERO : props.baseCost();
    }

    private String pickCourier(Delivery delivery) {
        List<String> names = props.courier().names();
        return names.get((int) (delivery.getId() % names.size()));
    }

    private LocalDateTime nextStepAt() {
        return LocalDateTime.now().plus(props.courier().step());
    }

    private static ResponseStatusException notFound(long id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Доставка " + id + " не найдена");
    }
}
