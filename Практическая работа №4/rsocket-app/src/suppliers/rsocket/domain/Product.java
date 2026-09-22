package suppliers.rsocket.domain;

public class Product {

    private int id;
    private String name;
    private int supplierId;
    private double price;
    private int quantity;

    public Product() {
    }

    public Product(int id, String name, int supplierId, double price, int quantity) {
        this.id = id;
        this.name = name;
        this.supplierId = supplierId;
        this.price = price;
        this.quantity = quantity;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(int supplierId) {
        this.supplierId = supplierId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "Product{id=" + id + ", name='" + name + "', supplierId=" + supplierId
                + ", price=" + price + ", quantity=" + quantity + '}';
    }
}
