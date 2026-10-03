package baas.orders.domain;

import jakarta.persistence.Embeddable;
import jakarta.validation.constraints.NotBlank;

/**
 * Embeddable-класс адреса (город, улица, здание, квартира).
 * Используется и как основной адрес клиента, и как адрес доставки заказа.
 */
@Embeddable
public class Address {

    @NotBlank
    private String city;

    @NotBlank
    private String street;

    @NotBlank
    private String building;

    private String flat;

    protected Address() {
    }

    public Address(String city, String street, String building, String flat) {
        this.city = city;
        this.street = street;
        this.building = building;
        this.flat = flat;
    }

    /** Embeddable нельзя разделять между сущностями, поэтому адрес копируется. */
    public Address copy() {
        return new Address(city, street, building, flat);
    }

    public String getCity() {
        return city;
    }

    public String getStreet() {
        return street;
    }

    public String getBuilding() {
        return building;
    }

    public String getFlat() {
        return flat;
    }
}
