package io.distmap.persistent;

import com.hazelcast.config.Config;
import io.distmap.ConfigManagement;
import io.distmap.DistributedMap;

/**
 * Created by mich8bsp on 25-Feb-16.
 */
public class PersistentDistributedMap {

    public static class MapBuilder<K, V> extends DistributedMap.MapBuilder<K, V>{

        public MapBuilder(String mapName, int domain) {
            super(mapName, domain);
        }

        @Override
        public Config getConfig(){
            Config config = super.getConfig();
            return ConfigManagement.addPersistence(config);
        }

    }

}
