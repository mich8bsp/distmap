package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.distmap.EMapPermissions;

import java.util.Map;

/**
 * Created by mich8bsp on 14-Jan-16.
 */
public class NetAPI {

    private final int environment;
    private HazelcastInstance hazelcast;

    public NetAPI(int environment, String partition) {
        this.environment = environment;
        Config config = ConfigManagement.initializeConfig(environment, partition);
        this.hazelcast = Hazelcast.newHazelcastInstance(config);
    }


    public <K,V> Map<K, V> getDistributedMap(String mapName, EMapPermissions permissions) {
        return hazelcast.getMap(mapName);
    }

}
