package darkstore.reactive.model;

/**
 * Результат преобразования SupplyRecord: доступный (quantity > 0) товар
 * с рассчитанной стоимостью партии (price * quantity).
 */
public class ProcessedItem {
    private final int supplierId;
    private final String supplierName;
    private final String product;
    private final double totalCost;

    public ProcessedItem(int supplierId, String supplierName, String product, double totalCost) {
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.product = product;
        this.totalCost = totalCost;
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

    public double getTotalCost() {
        return totalCost;
    }

    @Override
    public String toString() {
        return "ProcessedItem{supplierId=" + supplierId +
                ", supplierName='" + supplierName + '\'' +
                ", product='" + product + '\'' +
                ", totalCost=" + totalCost + '}';
    }
}
