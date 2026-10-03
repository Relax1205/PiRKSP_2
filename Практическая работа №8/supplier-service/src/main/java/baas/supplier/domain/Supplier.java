package baas.supplier.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Поставщик: юридическое лицо со своим складом. */
@Entity
@Table(name = "supplier")
public class Supplier {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, length = 12)
    private String inn;

    @Column(nullable = false)
    private String warehouseAddress;

    protected Supplier() {
    }

    public Supplier(String name, String inn, String warehouseAddress) {
        this.name = name;
        this.inn = inn;
        this.warehouseAddress = warehouseAddress;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getInn() {
        return inn;
    }

    public String getWarehouseAddress() {
        return warehouseAddress;
    }
}
