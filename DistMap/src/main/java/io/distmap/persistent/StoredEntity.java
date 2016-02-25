package io.distmap.persistent;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

/**
 * Created by mich8bsp on 25-Feb-16.
 */
@Entity
public class StoredEntity<K, V> {

    private static final String KEY_FIELD_NAME = "key";

    @Id
    protected K key;

    @Embedded
    protected V value;

    public StoredEntity(K key, V value){
        this.key = key;
        this.value = value;
    }

    public static String getKeyFieldName(){
        return KEY_FIELD_NAME;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
