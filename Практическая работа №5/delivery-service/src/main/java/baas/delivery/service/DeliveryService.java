package baas.delivery.service;

import baas.delivery.client.OrderClient;
import baas.delivery.client.OrderView;
import baas.delivery.config.DeliveryProperties;
import baas.delivery.domain.DeliveryStatus;
import baas.delivery.domain.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Формирует информацию о доставке по данным заказа из Order Service.
 * Сервис не хранит состояние: все данные живут в Order Service, поэтому
 * его можно запускать в любом количестве экземпляров за балансировщиком.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    /** Заказы, которые находятся «в работе» у службы доставки. */
    private static final List<OrderStatus> ACTIVE = List.of(OrderStatus.ASSEMBLY, OrderStatus.DELIVERY);

    private final OrderClient orderClient;
    private final DeliveryProperties tariffs;

    public DeliveryService(OrderClient orderClient, DeliveryProperties tariffs) {
        this.orderClient = orderClient;
        this.tariffs = tariffs;
    }

    public DeliveryInfo getDelivery(long orderId) {
        log.info("Запрос информации о доставке заказа {} -> Order Service", orderId);
        return toDelivery(orderClient.getOrder(orderId));
    }

    public List<DeliveryInfo> getActiveDeliveries() {
        log.info("Запрос активных доставок (статусы {}) -> Order Service", ACTIVE);
        return orderClient.getOrders(ACTIVE).stream().map(this::toDelivery).toList();
    }

    /** Курьер забрал заказ: ASSEMBLY -> DELIVERY. */
    public DeliveryInfo dispatch(long orderId) {
        log.info("Передача заказа {} курьеру", orderId);
        return toDelivery(orderClient.changeStatus(orderId, OrderStatus.DELIVERY));
    }

    /** Заказ вручён клиенту: DELIVERY -> COMPLETED. */
    public DeliveryInfo complete(long orderId) {
        log.info("Заказ {} доставлен", orderId);
        return toDelivery(orderClient.changeStatus(orderId, OrderStatus.COMPLETED));
    }

    private DeliveryInfo toDelivery(OrderView order) {
        DeliveryStatus status = DeliveryStatus.of(order.status());
        BigDecimal total = order.total() == null ? BigDecimal.ZERO : order.total();
        BigDecimal deliveryCost = total.compareTo(tariffs.freeThreshold()) >= 0 ? BigDecimal.ZERO : tariffs.baseCost();
        LocalDateTime eta = status.isFinal() ? null : order.orderDateTime().plusMinutes(tariffs.estimatedMinutes());
        int itemsCount = order.items() == null ? 0
                : order.items().stream().mapToInt(OrderView.Item::quantity).sum();
        OrderView.PersonalData person = order.customer().personalData();

        return new DeliveryInfo(
                order.id(),
                status,
                order.status(),
                person.lastName() + " " + person.firstName(),
                order.deliveryAddress(),
                order.deliveryAddress().line(),
                order.comment(),
                itemsCount,
                total,
                deliveryCost,
                total.add(deliveryCost),
                order.orderDateTime(),
                eta);
    }
}
