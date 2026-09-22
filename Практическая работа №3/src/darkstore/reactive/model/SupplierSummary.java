package darkstore.reactive.model;

/** Агрегат по поставщику: суммарная стоимость доступных товаров и их количество позиций. */
public class SupplierSummary {
    private final int supplierId;
    private final String supplierName;
    private final double totalValue;
    private final int itemCount;

    public SupplierSummary(int supplierId, String supplierName, double totalValue, int itemCount) {
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.totalValue = totalValue;
        this.itemCount = itemCount;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public double getTotalValue() {
        return totalValue;
    }

    public int getItemCount() {
        return itemCount;
    }

    @Override
    public String toString() {
        return "SupplierSummary{supplierId=" + supplierId +
                ", supplierName='" + supplierName + '\'' +
                ", totalValue=" + totalValue +
                ", itemCount=" + itemCount + '}';
    }
}
