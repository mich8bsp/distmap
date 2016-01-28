package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main builder for distributed map. should be used in the following way:
 * new DistributedMap.MapBuilder<K, V>(%MAP_NAME%).build();
 * Created by mich8bsp on 14-Jan-16.
 */
public class DistributedMap<K, V> {

    protected HazelcastInstance hazelcast;
    protected IMap<K, V> innerMap;

    protected AtomicBoolean initialized = new AtomicBoolean(false);

    public HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }

    public IMap<K, V> getMap() {
        return innerMap;
    }

    public class MapBuilder {

        private String mapName;
        private MapCallback<K, V> callback;
        private String partition;
        //        private HazelcastInstance hazelcast;
        private Predicate<K, V> callbackFilter;
        private int domain;

        public MapBuilder(String mapName, int domain) {
            if(initialized.get()){
                throw new IllegalStateException("Map was already built. Create new Distributed map");
            }
            this.mapName = mapName;
            this.domain = domain;
        }

        public MapBuilder setMapName(String mapName) {
            this.mapName = mapName;
            return this;
        }

        public MapBuilder setListener(MapCallback<K, V> callback) {
            this.callback = callback;
            return this;
        }

        public MapBuilder setListener(MapCallback<K, V> callback, Predicate<K, V> filter) {
            this.callback = callback;
            this.callbackFilter = filter;
            return this;
        }

        public MapBuilder setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public IMap<K, V> build() {
            if (hazelcast == null) {
                Config config = ConfigManagement.initializeConfig(domain, partition);
                hazelcast = Hazelcast.newHazelcastInstance(config);
            }
            IMap<K, V> map = hazelcast.getMap(mapName);
            if (callback != null) {
                if (callbackFilter == null) {
                    map.addEntryListener(callback, true);
                } else {
                    map.addEntryListener(callback, callbackFilter, true);
                }
            }
            innerMap = map;
            return map;
        }
    }


    public void close() {
        hazelcast.shutdown();
    }
}
