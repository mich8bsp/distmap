package io.distmap.redis;

/**
 * Created by mich8bsp on 07-Mar-16.
 */
public interface ICallback<K, V> {

    void onDataArrival(K key, V value);

    void onDataRemoval(K key, V value);
}
