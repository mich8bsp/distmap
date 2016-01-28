package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MulticastConfig;

/**
 * Config management for network properties
 * Created by mich8bsp on 14-Jan-16.
 */
public class ConfigManagement {

    protected static Config initializeConfig(int domain, String partition) {
        Config config = new Config();
        config.getGroupConfig().setName(partition);
        int multicastPort = getMulticastPortFromDomain(domain);
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(multicastPort);
        return config;
    }

    private static int getMulticastPortFromDomain(int domain) {
        return MulticastConfig.DEFAULT_MULTICAST_PORT + domain;
    }
}
