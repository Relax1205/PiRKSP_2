package baas.orders.domain;

import jakarta.persistence.Embeddable;
import jakarta.validation.constraints.NotBlank;

/** Embeddable-класс персональных данных клиента. */
@Embeddable
public class PersonalData {

    @NotBlank
    private String lastName;

    @NotBlank
    private String firstName;

    protected PersonalData() {
    }

    public PersonalData(String lastName, String firstName) {
        this.lastName = lastName;
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getFirstName() {
        return firstName;
    }
}
