import io.distmap.persistent.vertx.Key;

import java.util.Arrays;

/**
 * Created by mich8bsp on 27-Mar-16.
 */
public class ComplexTestType extends TestType implements IComplexTestType{

    private boolean someBoolean;
    private Integer[] someArray = new Integer[]{3,4,2,512};

    @Key
    private int secondaryKey;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ComplexTestType that = (ComplexTestType) o;

        if (someBoolean != that.someBoolean) return false;
        if (secondaryKey != that.secondaryKey) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(someArray, that.someArray);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (someBoolean ? 1 : 0);
        result = 31 * result + Arrays.hashCode(someArray);
        result = 31 * result + secondaryKey;
        return result;
    }

    public boolean isSomeBoolean() {
        return someBoolean;
    }

    public void setSomeBoolean(boolean someBoolean) {
        this.someBoolean = someBoolean;
    }

    public Integer[] getSomeArray() {
        return someArray;
    }

    public void setSomeArray(Integer[] someArray) {
        this.someArray = someArray;
    }

    public int getSecondaryKey() {
        return secondaryKey;
    }

    public void setSecondaryKey(int secondaryKey) {
        this.secondaryKey = secondaryKey;
    }

    @Override
    public String toString() {
        return "ComplexTestType{" +
                "someBoolean=" + someBoolean +
                ", someArray=" + Arrays.toString(someArray) +
                ", secondaryKey=" + secondaryKey +
                '}';
    }
}
