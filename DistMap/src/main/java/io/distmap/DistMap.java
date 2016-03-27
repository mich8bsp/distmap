package io.distmap;

import io.distmap.persistent.AbstractMapStore;
import io.distmap.persistent.DBInfo;
import io.distmap.persistent.PersistentDistributedMap;

/**
 * Created by mich8bsp on 27-Mar-16.
 */
public class DistMap {

    public static <K,V> DistributedMap.MapBuilder<K,V> mapBuilder(String mapName, int domainId){
        return new DistributedMap.MapBuilder<>(mapName, domainId);
    }

    public static <K,V> PersistentDistributedMap.PersistentMapBuilder<K,V> persistentMapBuilder(String mapName, int domainId, AbstractMapStore<K, V> mapStore, DBInfo dbInfo){
        return new PersistentDistributedMap.PersistentMapBuilder<>(mapName, domainId, mapStore, dbInfo);
    }
}
