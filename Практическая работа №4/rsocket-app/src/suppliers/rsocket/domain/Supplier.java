package suppliers.rsocket.domain;

public class Supplier {

    private int id;
    private String name;
    private String inn;
    private double rating;

    public Supplier() {
    }

    public Supplier(int id, String name, String inn, double rating) {
        this.id = id;
        this.name = name;
        this.inn = inn;
        this.rating = rating;
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

    public String getInn() {
        return inn;
    }

    public void setInn(String inn) {
        this.inn = inn;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    @Override
    public String toString() {
        return "Supplier{id=" + id + ", name='" + name + "', inn='" + inn + "', rating=" + rating + '}';
    }
}
