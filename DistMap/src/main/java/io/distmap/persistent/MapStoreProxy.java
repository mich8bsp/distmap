package io.distmap.persistent;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public class MapStoreProxy {

    public static <K, V> Map<K, V> toMap(AbstractMapStore<K, V> mapStore) {
        return new Map<K, V>() {
            @Override
            public int size() {
                return mapStore.size();
            }

            @Override
            public boolean isEmpty() {
                return mapStore.size() == 0;
            }

            @Override
            public boolean containsKey(Object key) {
                return mapStore.load((K) key) != null;
            }

            @Override
            public boolean containsValue(Object value) {
                return values().contains(value);
            }

            @Override
            public V get(Object key) {
                return mapStore.load((K) key);
            }

            @Override
            public V put(K key, V value) {
                mapStore.store(key, value);
                return value;
            }

            @Override
            public V remove(Object key) {
                V value = mapStore.load((K) key);
                mapStore.delete((K) key);
                return value;
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                mapStore.storeAllExt(m);
            }

            @Override
            public void clear() {
                mapStore.deleteAll(keySet());
            }

            @Override
            public Set<K> keySet() {
                Set<K> keys = new HashSet<>();
                mapStore.loadAllKeys().forEach(keys::add);
                return keys;
            }

            @Override
            public Collection<V> values() {
                Set<Entry<K, V>> entries = entrySet();
                return entries.stream().map(Entry::getValue).collect(Collectors.toList());
            }

            @Override
            public Set<Entry<K, V>> entrySet() {
                return mapStore.getEntrySet();
            }
        };
    }
}
