package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;

/** Main builder for distributed map. should be used in the following way:
 * new DistributedMap.MapBuilder<K, V>(%MAP_NAME%).build();
 * Created by mich8bsp on 14-Jan-16.
 */
public class DistributedMap {

    public static class MapBuilder<K, V> {

        private String mapName;
        private MapCallback<K,V> callback;
        private String partition;
        private HazelcastInstance hazelcast;
        private Predicate<K, V> callbackFilter;
        private int domain;

        public MapBuilder(String mapName, int domain) {
            this.mapName = mapName;
            this.domain = domain;
        }

        public MapBuilder<K, V> setMapName(String mapName) {
            this.mapName = mapName;
            return this;
        }

        public MapBuilder<K, V> setListener(MapCallback<K,V> callback) {
            this.callback = callback;
            return this;
        }

        public MapBuilder<K, V> setListener(MapCallback<K,V> callback, Predicate<K, V> filter) {
            this.callback = callback;
            this.callbackFilter = filter;
            return this;
        }

        public MapBuilder<K, V> setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public IMap<K, V> build() {
            if (hazelcast == null) {
                Config config = ConfigManagement.initializeConfig(domain, partition);
                this.hazelcast = Hazelcast.newHazelcastInstance(config);
            }
            IMap<K, V> map = hazelcast.getMap(mapName);
            if (callback != null) {
                if (callbackFilter == null) {
                    map.addEntryListener(callback, true);
                } else {
                    map.addEntryListener(callback, callbackFilter, true);
                }
            }
            return map;
        }


    }
}
