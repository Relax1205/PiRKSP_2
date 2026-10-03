package baas.orders.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Агрегат «Заказ». Корень агрегата, владеет позициями {@link OrderList}
 * (связь «один ко многим»).
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Внешняя ссылка на агрегат «Клиент». */
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "customer_id")
    private Customer customer;

    @Column(columnDefinition = "text")
    private String comment;

    @Column(nullable = false)
    private LocalDateTime orderDateTime;

    @Embedded
    private Address deliveryAddress;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private OrderStatus status;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("id")
    private List<OrderList> orderListList = new ArrayList<>();

    protected Order() {
    }

    public Order(Customer customer, String comment, Address deliveryAddress, LocalDateTime orderDateTime) {
        this.customer = customer;
        this.comment = comment;
        this.deliveryAddress = deliveryAddress;
        this.orderDateTime = orderDateTime;
        this.status = OrderStatus.DRAFT;
    }

    public void addItem(Product product, int quantity) {
        orderListList.add(new OrderList(this, product, quantity));
    }

    public void changeStatus(OrderStatus next) {
        if (!status.canMoveTo(next)) {
            throw new IllegalStateException(
                    "Недопустимый переход статуса заказа " + id + ": " + status + " -> " + next
                            + " (допустимо: " + status.nextStatuses() + ")");
        }
        status = next;
    }

    public BigDecimal total() {
        return orderListList.stream()
                .map(OrderList::sum)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public Long getId() {
        return id;
    }

    public Customer getCustomer() {
        return customer;
    }

    public String getComment() {
        return comment;
    }

    public LocalDateTime getOrderDateTime() {
        return orderDateTime;
    }

    public Address getDeliveryAddress() {
        return deliveryAddress;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public List<OrderList> getOrderListList() {
        return orderListList;
    }
}
