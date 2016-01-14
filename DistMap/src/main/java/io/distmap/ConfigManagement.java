package io.distmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;

/**
 * Created by mich8bsp on 14-Jan-16.
 */
public class ConfigManagement {
    private static final String MULTICAST_ADDRESS = "239.255.0.1";
    private static final int RANGE = 15;
    private static final int FACTOR = 250;
    private static final int BASE_PORT = 7800;
    private static final int MULTICAST_TIMEOUT = 200;
    private static EConnectionType connectionType = EConnectionType.MULTICAST;

    protected static Config initializeConfig(int environment, String partition) {
        Config config = new Config();
        NetworkConfig netConfig = config.getNetworkConfig();
        int port = envToPort(environment,connectionType);
        netConfig.setPort(port);

        JoinConfig join = netConfig.getJoin();
        join.getTcpIpConfig().setEnabled(connectionType.equals(EConnectionType.UNICAST));
        join.getAwsConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(connectionType.equals(EConnectionType.UNICAST));

        if(connectionType.equals(EConnectionType.MULTICAST)) {
            join.getMulticastConfig().setMulticastGroup(MULTICAST_ADDRESS);
            join.getMulticastConfig().setMulticastPort(envToPort(environment, EConnectionType.MULTICAST));
            join.getMulticastConfig().setMulticastTimeoutSeconds(MULTICAST_TIMEOUT);
        }
        return config;
    }

    private static int envToPort(int environment, EConnectionType portType) {
        int port = BASE_PORT + (FACTOR * environment);
        if(portType.equals(EConnectionType.MULTICAST)){
            port += RANGE;
        }
        return port;
    }



}
