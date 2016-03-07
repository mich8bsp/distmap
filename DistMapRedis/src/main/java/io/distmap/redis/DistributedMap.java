package io.distmap.redis;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.RedissonClient;
import org.redisson.core.RMap;
import org.redisson.core.RTopic;

import java.util.Map;

/**
 * Created by mich8bsp on 06-Mar-16.
 */
public class DistributedMap<K, V> {

    private String mapName;
    private RedissonClient redisson;
    private int domainId;
    private String partition = "*";
    private static final int BASE_PORT = 61234;
    private static final String SEPARATOR = "_";
    private ICallback<K, V> callback;
    private boolean initialized = false;

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
            return redisson.getMap(mapName);
        }
        mapName = mapName + SEPARATOR + partition;
        initialized = true;
        redisson = Redisson.create(initConfig(domainId));
        if(callback!=null){
            createTopicListener();
        }
        return createProxy(redisson.<K,V>getMap(mapName));
    }

    private RMapProxy<K, V> createProxy(RMap<K, V> map) {
        return new RMapProxy<>(map, redisson.<NotificationMsg<K, V>>getTopic(mapName));
    }

    private void createTopicListener() {
        RTopic<NotificationMsg<K,V>> listenerTopic = redisson.getTopic(mapName);
        listenerTopic.addListener((channel, msg) -> {
            if (msg.getStatus().equals(UpdateStatus.NEW) || msg.getStatus().equals(UpdateStatus.UPDATE)) {
                callback.onDataArrival(msg.getUpdate().getKey(), msg.getUpdate().getValue());
            } else if (msg.getStatus().equals(UpdateStatus.REMOVE)) {
                callback.onDataRemoval(msg.getUpdate().getKey(), msg.getUpdate().getValue());
            }
        });
    }

    private static Config initConfig(int domainId) {
        Config config = new Config();
        config.useSingleServer().setAddress("localhost:" + (BASE_PORT + domainId));
        return config;
    }
}
