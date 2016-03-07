package io.distmap.persistent;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.MapStore;
import io.distmap.ConfigManagement;
import io.distmap.DistributedMap;

/**
 * Created by mich8bsp on 25-Feb-16.
 */
public class PersistentDistributedMap {

    public static class MapBuilder<K, V> extends DistributedMap.MapBuilder<K, V>{

        private final MapStore<K, V> mapStore;
        private MapStoreConfig.InitialLoadMode loadMode = MapStoreConfig.InitialLoadMode.LAZY;
        private DBInfo dbInfo;


        public MapBuilder(String mapName, int domain, MapStore<K, V> mapStore, DBInfo dbInfo)
        {
            super(mapName, domain);
            this.mapStore = mapStore;
            this.dbInfo = dbInfo;
        }

        private MapBuilder<K, V> setInitialLoadMode(MapStoreConfig.InitialLoadMode loadMode){
            this.loadMode = loadMode;
            return this;
        }

        @Override
        public Config getConfig(){
            Config config = super.getConfig();
            return ConfigManagement.addPersistence(config, mapName, mapStore, loadMode, dbInfo);
        }

    }

}
