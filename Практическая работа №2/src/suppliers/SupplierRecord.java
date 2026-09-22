package suppliers;

import java.math.BigDecimal;

/** Одна строка файла обмена: товар конкретного поставщика. */
public final class SupplierRecord {
    private final int supplierId;
    private final String supplierName;
    private final String product;
    private final BigDecimal price;
    private final int quantity;

    public SupplierRecord(int supplierId, String supplierName, String product, BigDecimal price, int quantity) {
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.product = product;
        this.price = price;
        this.quantity = quantity;
    }

    public int getSupplierId() { return supplierId; }
    public String getSupplierName() { return supplierName; }
    public String getProduct() { return product; }
    public BigDecimal getPrice() { return price; }
    public int getQuantity() { return quantity; }

    /** Стоимость всей партии = цена * количество. */
    public BigDecimal getTotal() {
        return price.multiply(BigDecimal.valueOf(quantity));
    }

    /**
     * Разбор строки CSV (разделитель - запятая, 5 полей).
     * @throws IllegalArgumentException если строка некорректна
     */
    public static SupplierRecord parse(String line) {
        String[] f = line.split(",", -1);
        if (f.length != 5) {
            throw new IllegalArgumentException("ожидается 5 полей, найдено " + f.length);
        }
        int id;
        BigDecimal price;
        int qty;
        try {
            id = Integer.parseInt(f[0].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("некорректное число в supplierId: \"" + f[0].trim() + "\"");
        }
        try {
            price = new BigDecimal(f[3].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("некорректное число в price: \"" + f[3].trim() + "\"");
        }
        try {
            qty = Integer.parseInt(f[4].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("некорректное число в quantity: \"" + f[4].trim() + "\"");
        }
        String name = f[1].trim();
        String product = f[2].trim();
        if (id <= 0) throw new IllegalArgumentException("supplierId должен быть > 0");
        if (name.isEmpty()) throw new IllegalArgumentException("пустое supplierName");
        if (product.isEmpty()) throw new IllegalArgumentException("пустое поле product");
        if (price.signum() < 0) throw new IllegalArgumentException("отрицательная цена");
        if (qty < 0) throw new IllegalArgumentException("отрицательное количество");
        return new SupplierRecord(id, name, product, price, qty);
    }
}
