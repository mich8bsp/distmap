import java.util.List;

/**
 * Created by mich8bsp on 06-Mar-16.
 */
public class TestEntity {
    private int id;
    private List<Integer> points;
    private String name;

    public TestEntity(){

    }
    public TestEntity(int id, List<Integer> points, String name) {
        this.id = id;
        this.points = points;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<Integer> getPoints() {
        return points;
    }

    public void setPoints(List<Integer> points) {
        this.points = points;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestEntity that = (TestEntity) o;

        if (id != that.id) return false;
        if (points != null ? !points.equals(that.points) : that.points != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (points != null ? points.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TestEntity{" +
                "id=" + id +
                ", points=" + points +
                ", name='" + name + '\'' +
                '}';
    }
}
