package io.distmap.persistent;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mongodb.ServerAddress;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public abstract class AbstractMapStore<K, V> implements MapStore<K, V>, MapLoaderLifecycleSupport {

    public static final int DEFAULT_MONGO_PORT = 27017;
    private String collectionName;

    private static final String KEYS_COLLECTION_SUFFIX = "_keys";
    private static final String VALUES_COLLECTION_SUFFIX = "_values";
    public Map.Entry<Class<? extends K>, Class<? extends V>> typesEntry;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.collectionName = mapName;
        DBInfo dbInfo = DBInfo.getDBInfo(properties);
        connectToDB(dbInfo);
    }

    public abstract void connectToDB(DBInfo dbInfo);


    public String getKeysCollectionName(){
        return collectionName + KEYS_COLLECTION_SUFFIX;
    }

    public String getValuesCollectionName(){
        return collectionName + VALUES_COLLECTION_SUFFIX;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public void storeAll(Map<K, V> map) {
        storeAllExt(map);
    }

    public void storeAllExt(Map<? extends K, ? extends V> map){
        map.entrySet().stream().forEach(x -> store(x.getKey(), x.getValue()));
    }


    @Override
    public Map<K, V> loadAll(Collection<K> collection) {
        Map<K, V> loadedMap = new HashMap<>(collection.size());
        collection.forEach(x -> {
            V value = load(x);
            loadedMap.put(x, value);
        });
        return loadedMap;
    }

    public static List<ServerAddress> parseHosts(List<String> dbHosts) {
        if (dbHosts == null || dbHosts.size() == 0) {
            return Collections.singletonList(new ServerAddress("localhost", DEFAULT_MONGO_PORT));
        }
        return dbHosts.stream().map(x -> {
            String[] ipAndHost = x.split(":");
            String host = ipAndHost[0];
            int port = DEFAULT_MONGO_PORT;
            if (ipAndHost.length > 1) {
                port = Integer.parseInt(ipAndHost[1]);
            }
            return new ServerAddress(host, port);
        }).collect(Collectors.toList());
    }

    @Override
    public void deleteAll(Collection<K> keys){
        keys.forEach(this::delete);
    }

    public Collection<V> loadAllValues() {
        return getEntrySet().stream().map(Map.Entry::getValue).collect(Collectors.toSet());
    }

    public boolean containsValue(Object value) {
       return loadAllValues().contains(value);
    }

    public abstract int size();
    public abstract Set<Map.Entry<K, V>> getEntrySet();
    public abstract boolean containsKey(K key);
    public abstract void deleteAll();


}
