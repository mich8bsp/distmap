package io.distmap.persistent.morphia;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

/**
 * Created by mich8bsp on 25-Feb-16.
 */
@Entity
public class StoredEntity{

    @Id
    protected IStorable key;

    @Embedded
    protected IStorable value;

    public StoredEntity(IStorable key, IStorable value){
        this.key = key.getKey();
        this.value = value;
    }

    public IStorable getKey() {
        return key;
    }

    public void setKey(IStorable key) {
        this.key = key;
    }

    public IStorable getValue() {
        return value;
    }

    public void setValue(IStorable value) {
        this.value = value;
    }
}
