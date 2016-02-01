package io.distmap;

import com.hazelcast.config.*;

/**
 * Config management for network properties
 * Created by mich8bsp on 14-Jan-16.
 */
public class ConfigManagement {

    private static int getMulticastPortFromDomain(int domain) {
        return MulticastConfig.DEFAULT_MULTICAST_PORT + domain;
    }

    public static <K, V> Config initializeConfig(DistributedMap<K, V>.MapBuilder mapBuilder) {
        Config config = new XmlConfigBuilder().build();

        config.getGroupConfig().setName(mapBuilder.getPartition());
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        TcpIpConfig unicastConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
        if(mapBuilder.getHosts()!=null && mapBuilder.getHosts().size()>0){
            multicastConfig.setEnabled(false);
            unicastConfig.setEnabled(true);
            unicastConfig.setMembers(mapBuilder.getHosts());
        }else {
            int multicastPort = getMulticastPortFromDomain(mapBuilder.getDomain());
            multicastConfig.setEnabled(true);
            unicastConfig.setEnabled(false);
            multicastConfig.setMulticastPort(multicastPort);
        }
        return config;
    }
}
