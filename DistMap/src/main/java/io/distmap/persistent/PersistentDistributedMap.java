package io.distmap.persistent;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import io.distmap.ConfigManagement;
import io.distmap.DistributedMap;

import java.util.Map;

/**
 * Created by mich8bsp on 25-Feb-16.
 */
public class PersistentDistributedMap {

    public static class PersistentMapBuilder<K, V> extends DistributedMap.MapBuilder<K, V> {

        private final AbstractMapStore<K, V> mapStore;
        private MapStoreConfig.InitialLoadMode loadMode = MapStoreConfig.InitialLoadMode.LAZY;
        private DBInfo dbInfo;
        private boolean directToDB;

        public PersistentMapBuilder(String mapName, int domain, AbstractMapStore<K, V> mapStore, DBInfo dbInfo) {
            super(mapName, domain);
            this.mapStore = mapStore;
            this.dbInfo = dbInfo;
        }

        public PersistentMapBuilder<K, V> setInitialLoadMode(MapStoreConfig.InitialLoadMode loadMode) {
            this.loadMode = loadMode;
            return this;
        }

        public PersistentMapBuilder<K, V> setDirectToDB(boolean directToDB) {
            this.directToDB = directToDB;
            return this;
        }

        @Override
        public Config getConfig() {
            Config config = super.getConfig();
            return ConfigManagement.addPersistence(config, mapName, mapStore, loadMode, dbInfo);
        }

        @Override
        public Map<K, V> build() {
            dbInfo.appendDBNameEnvironment("_"+domain+"_"+partition);
            if (!directToDB) {
                return super.build();
            } else {
                mapStore.connectToDB(dbInfo);
                mapStore.setCollectionName(mapName);
                return MapStoreProxy.toMap(mapStore);
            }
        }

    }

}
