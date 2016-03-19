package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.MapStore;
import io.distmap.persistent.DBInfo;

/**
 * Config management for network properties
 * Created by mich8bsp on 14-Jan-16.
 */
public class ConfigManagement {

    public static Config initializeConfig(int domain, String partition) {
        Config config = new Config();
        config.getGroupConfig().setName(partition);
        int multicastPort = getMulticastPortFromDomain(domain);
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(multicastPort);
        return config;
    }

    public static Config addPersistence(Config config, String mapName, MapStore mapStore, MapStoreConfig.InitialLoadMode loadMode, DBInfo dbInfo) {
        MapConfig mapConfig = config.getMapConfig(mapName);
        if(mapConfig==null){
            mapConfig = new MapConfig();
            mapConfig.setName(mapName);
            config.addMapConfig(mapConfig);
        }

        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if(mapStoreConfig==null){
            mapStoreConfig = new MapStoreConfig();
            mapConfig.setMapStoreConfig(mapStoreConfig);
        }
        mapStoreConfig.setClassName(mapStore.getClass().getName());
        mapStoreConfig.setImplementation(mapStore);
        mapStoreConfig.setInitialLoadMode(loadMode);
        mapStoreConfig.setProperties(DBInfo.getProperties(dbInfo));
        return config;
    }

    private static int getMulticastPortFromDomain(int domain) {
        return MulticastConfig.DEFAULT_MULTICAST_PORT + domain;
    }
}
