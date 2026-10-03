package baas.delivery.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Доставка заказа. Данные о заказе (получатель, адрес, сумма) пришли в событии
 * OrderCreated и скопированы сюда: к БД Order Service этот сервис не обращается.
 */
@Entity
@Table(name = "delivery")
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Уникальный: на один заказ — одна доставка (второй рубеж защиты от дублей). */
    @Column(name = "order_id", nullable = false, unique = true)
    private Long orderId;

    @Column(nullable = false)
    private String recipient;

    @Column(nullable = false, length = 500)
    private String address;

    @Column(columnDefinition = "text")
    private String comment;

    /** Стоимость доставки. */
    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal price;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private DeliveryStatus status;

    private String courier;

    /** X-Request-ID запроса, создавшего заказ: по нему видна вся история доставки в логах. */
    @Column(length = 64)
    private String requestId;

    /** Когда курьер сделает следующий шаг (null — доставка завершена). */
    private LocalDateTime nextStepAt;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @Version
    private Long version;

    protected Delivery() {
    }

    public Delivery(Long orderId, String recipient, String address, String comment, BigDecimal price,
                    String requestId, LocalDateTime nextStepAt) {
        this.orderId = orderId;
        this.recipient = recipient;
        this.address = address;
        this.comment = comment;
        this.price = price;
        this.status = DeliveryStatus.CREATED;
        this.requestId = requestId;
        this.nextStepAt = nextStepAt;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = createdAt;
    }

    public void moveTo(DeliveryStatus next, String courier, LocalDateTime nextStepAt) {
        if (!status.canMoveTo(next)) {
            throw new IllegalStateException("Недопустимый переход доставки " + id + ": " + status + " -> " + next
                    + " (допустимо: " + status.nextStatuses() + ")");
        }
        this.status = next;
        this.courier = courier;
        this.nextStepAt = nextStepAt;
        this.updatedAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getRecipient() {
        return recipient;
    }

    public String getAddress() {
        return address;
    }

    public String getComment() {
        return comment;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public String getCourier() {
        return courier;
    }

    public String getRequestId() {
        return requestId;
    }

    public LocalDateTime getNextStepAt() {
        return nextStepAt;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
}
