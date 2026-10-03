package baas.delivery.domain;

/** Адрес доставки в контракте Order Service. */
public record Address(String city, String street, String building, String flat) {

    public String line() {
        String result = "г. " + city + ", " + street + ", д. " + building;
        return flat == null || flat.isBlank() ? result : result + ", кв. " + flat;
    }
}
