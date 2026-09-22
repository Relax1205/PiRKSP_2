package darkstore.reactive.model;

/**
 * Исходная запись поставки, аналог сущности GoodsList (Good + Darkstore + count)
 * из модуля "Back-end АС Поставщиков" (bootcamp-darkstore-provider).
 */
public class SupplyRecord {
    private final int supplierId;
    private final String supplierName;
    private final String product;
    private final double price;
    private final int quantity;

    public SupplyRecord(int supplierId, String supplierName, String product, double price, int quantity) {
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.product = product;
        this.price = price;
        this.quantity = quantity;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public String getProduct() {
        return product;
    }

    public double getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "SupplyRecord{supplierId=" + supplierId +
                ", supplierName='" + supplierName + '\'' +
                ", product='" + product + '\'' +
                ", price=" + price +
                ", quantity=" + quantity + '}';
    }
}
