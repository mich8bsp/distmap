package io.distmap.persistent;

import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.mongodb.morphia.AdvancedDatastore;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by אלכס on 25/02/2016.
 */
public abstract class MongoMapStore<K, V> implements MapStore<K, V> {

    private static final ILogger logger = Logger.getLogger(MongoMapStore.class);
    public static final int DEFAULT_MONGO_PORT = 27017;
    private AdvancedDatastore datastore;


    public abstract Class<V> getStoredValueClass();

    public abstract Class<K> getStoredKeyClass();

    public String getCollectionName() {
        return getStoredValueClass().getName();
    }

    public void connectToDB(DBInfo dbInfo) {
        MongoClient client = createClient(dbInfo);
        Morphia morphia = new Morphia();
        mapEntities(morphia);
        datastore = (AdvancedDatastore) morphia.createDatastore(client, dbInfo.getDbName());
        datastore.ensureIndexes();
    }

    protected void mapEntities(Morphia morphia) {
        morphia.map(getStoredKeyClass());
        morphia.map(getStoredValueClass());
        morphia.map(StoredEntity.class);
    }

    private MongoClient createClient(DBInfo dbInfo) {
        MongoCredential cred = MongoCredential.createScramSha1Credential(dbInfo.getUserName(), dbInfo.getDbName(), dbInfo.getPassword().toCharArray());
        List<MongoCredential> creds = new LinkedList<>();
        creds.add(cred);
        return new MongoClient(parseHosts(dbInfo.getDbHosts()), creds);
    }

    private List<ServerAddress> parseHosts(List<String> dbHosts) {
        if (dbHosts == null || dbHosts.size() == 0) {
            return Collections.singletonList(new ServerAddress("localhost", DEFAULT_MONGO_PORT));
        }
        return dbHosts.stream().map(x -> {
            String[] ipAndHost = x.split(":");
            String host = ipAndHost[0];
            int port = DEFAULT_MONGO_PORT;
            if (ipAndHost.length > 1) {
                port = Integer.parseInt(ipAndHost[1]);
            }
            return new ServerAddress(host, port);
        }).collect(Collectors.toList());
    }

    @Override
    public void store(K key, V value) {
        StoredEntity storedEntity = new StoredEntity<>(key, value);
        datastore.save(getCollectionName(), storedEntity);
    }

    @Override
    public void storeAll(Map<K, V> map) {
        map.entrySet().stream().forEach(x -> store(x.getKey(), x.getValue()));
    }

    @Override
    public void delete(K key) {
        datastore.delete(getCollectionName(), StoredEntity.class, key);
    }

    @Override
    public void deleteAll(Collection<K> collection) {
        collection.forEach(this::delete);
    }

    @Override
    public V load(K key) {
        return (V) datastore.get(getCollectionName(), StoredEntity.class, key).getValue();
    }

    @Override
    public Map<K, V> loadAll(Collection<K> collection) {
        Map<K, V> loadedMap = new HashMap<>(collection.size());
        collection.forEach(x -> {
            V value = load(x);
            loadedMap.put(x, value);
        });
        return loadedMap;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        List<StoredEntity> allStored = datastore.find(getCollectionName(), StoredEntity.class).asList();
        return allStored.stream().map(x -> (K) x.getKey()).collect(Collectors.toSet());
    }
}
