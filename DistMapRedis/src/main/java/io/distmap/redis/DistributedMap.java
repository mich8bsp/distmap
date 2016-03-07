package io.distmap.redis;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.RedissonClient;
import org.redisson.core.RMap;
import org.redisson.core.RTopic;
import org.redisson.misc.URIBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by mich8bsp on 06-Mar-16.
 */
public class DistributedMap<K, V> {

    private static final String CONFIG_FILE = "dist-map-config.json";
    private String mapName;
    private RedissonClient redisson;
    private int domainId;
    private String partition = "*";
    private static final int BASE_PORT = 61234;
    private static final String SEPARATOR = "_";
    private ICallback<K, V> callback;
    private boolean initialized = false;
    private Map<K, V> innerMap;

    public DistributedMap(String mapName, int domainId) {
        this.domainId = domainId;
        this.mapName = mapName;
    }

    public DistributedMap<K, V> setPartition(String partition) {
        this.partition = partition;
        return this;
    }

    public DistributedMap<K, V> addListener(ICallback<K, V> callback) {
        this.callback = callback;
        return this;
    }

    public Map<K, V> build() {
        if (initialized) {
            return innerMap;
        }
        mapName = mapName + SEPARATOR + partition;
        redisson = Redisson.create(initConfig(domainId));
        if (callback != null) {
            createTopicListener();
        }
        innerMap = createProxy(redisson.<K, V>getMap(mapName));
        initialized = true;
        return innerMap;
    }

    private RMapProxy<K, V> createProxy(RMap<K, V> map) {
        return new RMapProxy<>(map, redisson.<NotificationMsg<K, V>>getTopic(mapName));
    }

    private void createTopicListener() {
        RTopic<NotificationMsg<K, V>> listenerTopic = redisson.getTopic(mapName);
        listenerTopic.addListener((channel, msg) -> {
            if (msg.getStatus().equals(UpdateStatus.NEW) || msg.getStatus().equals(UpdateStatus.UPDATE)) {
                callback.onDataArrival(msg.getUpdate().getKey(), msg.getUpdate().getValue());
            } else if (msg.getStatus().equals(UpdateStatus.REMOVE)) {
                callback.onDataRemoval(msg.getUpdate().getKey(), msg.getUpdate().getValue());
            }
        });
    }

    private static Config initConfig(int domainId) {
        Config config;
        try {
            config = Config.fromJSON(new File(CONFIG_FILE));
            if (config.useClusterServers() != null) {
                List<URI> addresses = config.useClusterServers().getNodeAddresses();
                List<URI> updatedAddresses = addresses.stream().map(x -> x.getHost() + ":" + (x.getPort() + domainId)).
                        map(URIBuilder::create).collect(Collectors.toList());
                addresses.clear();
                addresses.addAll(updatedAddresses);
            } else if (config.useSingleServer() != null) {
                URI address = config.useSingleServer().getAddress();
                String updatedAddress = address.getHost() + ":" + (address.getPort() + domainId);
                config.useSingleServer().setAddress(updatedAddress);
            }
        } catch (IOException e) {
            //FIXME: log error
            config = new Config();
            config.useSingleServer().setAddress("localhost:" + (BASE_PORT + domainId));
        }
        return config;
    }
}
