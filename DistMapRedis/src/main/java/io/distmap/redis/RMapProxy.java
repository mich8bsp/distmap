package io.distmap.redis;

import org.redisson.core.RMap;
import org.redisson.core.RTopic;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by mich8bsp on 07-Mar-16.
 */
public class RMapProxy<K, V> implements Map<K, V> {

    private final RMap<K, V> map;
    private final RTopic<NotificationMsg<K, V>> publishTopic;

    public RMapProxy(RMap<K, V> map, RTopic<NotificationMsg<K, V>> publishTopic) {
        this.map = map;
        this.publishTopic = publishTopic;
    }


    public V put(K key, V value) {
        createAndPublish(key, value, UpdateStatus.NEW);
        return map.put(key, value);
    }


    public V remove(Object key) {
        V oldValue = map.get(key);
        createAndPublish((K) key, oldValue, UpdateStatus.REMOVE);
        return map.remove(key);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        m.entrySet().forEach(x -> createAndPublish(x.getKey(), x.getValue(), UpdateStatus.NEW));
        map.putAll(m);
    }

    public void clear() {
        map.entrySet().forEach(x -> createAndPublish(x.getKey(), x.getValue(), UpdateStatus.REMOVE));
        map.clear();
    }

    private void createAndPublish(final K key, final V value, UpdateStatus status) {
        NotificationMsg<K, V> notification = new NotificationMsg<>();
        notification.setStatus(status);
        SimpleMapEntry<K, V> entry = new SimpleMapEntry<>();
        entry.setKey(key);
        entry.setValue(value);
        notification.setUpdate(entry);
        publishTopic.publish(notification);
    }

    /**
     * ---------------------------------------------------------------------
     */

    public Set<K> keySet() {
        return map.keySet();
    }

    public Collection<V> values() {
        return map.values();
    }

    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return map.equals(o);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    public V get(Object key) {
        return map.get(key);
    }
}
