package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.MapListener;

import java.util.Map;

/**
 * Created by mich8bsp on 14-Jan-16.
 */
public class DistributedMap {

    public static class MapBuilder<K,V> {

        private String mapName;
        private MapListener listener;
        private String partition;
        private HazelcastInstance hazelcast;

        public MapBuilder(String mapName) {
            this.mapName = mapName;
        }

        public MapBuilder<K, V> setMapName(String mapName) {
            this.mapName = mapName;
            return this;
        }

        public MapBuilder<K, V> setListener(MapListener listener) {
            this.listener = listener;
            return this;
        }

        public MapBuilder<K,V> setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public IMap<K, V> build() {
            if (hazelcast == null) {
                Config config = ConfigManagement.initializeConfig(partition);
                this.hazelcast = Hazelcast.newHazelcastInstance(config);
            }
            IMap<K, V> map = hazelcast.getMap(mapName);
            if (listener != null) {
                map.addEntryListener(listener, true);
            }
            return map;
        }


    }
}
