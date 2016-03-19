import io.distmap.persistent.morphia.IStorable;
import io.distmap.persistent.vertx.Keyed;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestType implements Serializable, Keyed{

    private int id;
    private String name ="testname";
    private String[] something;
    private List<Integer> listSomething;


    public TestType(){
       something = new String[]{"sda, asda"};
        listSomething = new ArrayList<>();
        listSomething.add(34);
        listSomething.add(342);
    }
    public int getId() {
        return id;
    }

    public TestType setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public TestType setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestType testType = (TestType) o;

        if (id != testType.id) return false;
        if (name != null ? !name.equals(testType.name) : testType.name != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(something, testType.something)) return false;
        return listSomething != null ? listSomething.equals(testType.listSomething) : testType.listSomething == null;

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(something);
        result = 31 * result + (listSomething != null ? listSomething.hashCode() : 0);
        return result;
    }

    public String[] getSomething() {

        return something;
    }

    public TestType setSomething(String[] something) {
        this.something = something;
        return this;
    }

    public List<Integer> getListSomething() {
        return listSomething;
    }

    public TestType setListSomething(List<Integer> listSomething) {
        this.listSomething = listSomething;
        return this;
    }

    @Override
    public Field[] getKeyFields() {
        try {
            return new Field[]{TestType.class.getDeclaredField("id")};
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            return new Field[0];
        }
    }
}
