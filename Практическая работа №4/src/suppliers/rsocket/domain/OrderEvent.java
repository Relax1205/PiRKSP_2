package suppliers.rsocket.domain;

/**
 * Событие, отправляемое клиентом на сервер по модели Fire-and-Forget:
 * клиент не ждёт подтверждения о том, что событие обработано.
 */
public class OrderEvent {

    private int productId;
    private int quantity;
    private String note;

    public OrderEvent() {
    }

    public OrderEvent(int productId, int quantity, String note) {
        this.productId = productId;
        this.quantity = quantity;
        this.note = note;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    @Override
    public String toString() {
        return "OrderEvent{productId=" + productId + ", quantity=" + quantity + ", note='" + note + "'}";
    }
}
