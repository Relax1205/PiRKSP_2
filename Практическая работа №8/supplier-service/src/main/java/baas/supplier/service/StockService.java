package baas.supplier.service;

import baas.supplier.domain.ProcessedEvent;
import baas.supplier.messaging.OrderCreatedEvent;
import baas.supplier.repository.ProcessedEventRepository;
import baas.supplier.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Списание товаров со склада по событию OrderCreated. */
@Service
public class StockService {

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private final ProductRepository products;
    private final ProcessedEventRepository processedEvents;

    public StockService(ProductRepository products, ProcessedEventRepository processedEvents) {
        this.products = products;
        this.processedEvents = processedEvents;
    }

    /**
     * Идемпотентно: списание и отметка «событие обработано» сохраняются в одной транзакции.
     * Повторно доставленное событие (тот же eventId) остатки не меняет.
     */
    @Transactional
    public void writeOff(OrderCreatedEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.warn("Повторное событие {} (OrderCreated, заказ {}) уже обработано — товары повторно не списываются",
                    event.eventId(), event.orderId());
            return;
        }
        for (OrderCreatedEvent.Item item : event.items()) {
            products.findById(item.productId()).ifPresentOrElse(product -> {
                int before = product.getStock();
                product.writeOff(item.quantity());
                log.info("Заказ {}: списано «{}» × {}, остаток {} → {}",
                        event.orderId(), product.getName(), item.quantity(), before, product.getStock());
            }, () -> log.warn("Заказ {}: товар {} не найден — пропущен", event.orderId(), item.productId()));
        }
        processedEvents.save(new ProcessedEvent(event.eventId(), event.type(), event.orderId()));
    }
}
