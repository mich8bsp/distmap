package io.distmap;

import com.hazelcast.config.Config;

/**
 * Created by mich8bsp on 14-Jan-16.
 */
public class ConfigManagement {

    protected static Config initializeConfig(String partition) {
        Config config = new Config();
        config.getGroupConfig().setName(partition);
        return config;
    }
}
