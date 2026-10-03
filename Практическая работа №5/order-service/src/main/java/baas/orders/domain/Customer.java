package baas.orders.domain;

import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Агрегат «Клиент»: персональные данные и основной адрес доставки. */
@Entity
@Table(name = "customer")
public class Customer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Embedded
    private PersonalData personalData;

    @Embedded
    private Address address;

    protected Customer() {
    }

    public Customer(PersonalData personalData, Address address) {
        this.personalData = personalData;
        this.address = address;
    }

    public Long getId() {
        return id;
    }

    public PersonalData getPersonalData() {
        return personalData;
    }

    public Address getAddress() {
        return address;
    }
}
