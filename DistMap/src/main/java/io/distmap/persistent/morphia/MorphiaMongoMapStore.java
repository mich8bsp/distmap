//package io.distmap.persistent.morphia;
//
//import com.mongodb.MongoClient;
//import com.mongodb.MongoCredential;
//import io.distmap.persistent.AbstractMapStore;
//import io.distmap.persistent.DBInfo;
//import org.mongodb.morphia.AdvancedDatastore;
//import org.mongodb.morphia.Morphia;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * Created by mich8bsp on 25/02/2016.
// */
//public abstract class MorphiaMongoMapStore<K extends IStorable, V extends IStorable> extends AbstractMapStore<K, V> {
//
//    private AdvancedDatastore datastore;
//    private MongoClient client;
//
//    @Override
//    public void destroy() {
//        client.close();
//    }
//
//
//    public void connectToDB(DBInfo dbInfo) {
//        client = createClient(dbInfo);
//        Morphia morphia = new Morphia();
//        mapEntities(morphia);
//        datastore = (AdvancedDatastore) morphia.createDatastore(client, dbInfo.getDbName());
//        datastore.ensureIndexes();
//    }
//
//    protected void mapEntities(Morphia morphia) {
//        mapClass(morphia, getStoredKeyClass());
//        mapClass(morphia, getStoredValueClass());
//        mapClass(morphia, StoredEntity.class);
//    }
//
//    protected void mapClass(Morphia morphia, Class<?> classToMap){
//        if(!morphia.isMapped(classToMap)){
//            morphia.map(classToMap);
//        }
//    }
//
//    private MongoClient createClient(DBInfo dbInfo) {
//        MongoCredential cred = MongoCredential.createScramSha1Credential(dbInfo.getUserName(), dbInfo.getDbName(), dbInfo.getPassword().toCharArray());
//        List<MongoCredential> creds = new LinkedList<>();
//        creds.add(cred);
//        return new MongoClient(parseHosts(dbInfo.getDbHosts()), creds);
//    }
//
//
//    @Override
//    public void store(K key, V value) {
//        StoredEntity storedEntity = new StoredEntity(key, value);
//        datastore.save(getCollectionName(), storedEntity);
//    }
//
//
//    @Override
//    public void delete(K key) {
//        datastore.delete(getCollectionName(), StoredEntity.class, key);
//    }
//
//    @Override
//    public V load(K key) {
//        return (V) datastore.get(getCollectionName(), StoredEntity.class, key).getValue();
//    }
//
//    @Override
//    public Iterable<K> loadAllKeys() {
//        List<StoredEntity> allStored = datastore.find(getCollectionName(), StoredEntity.class).asList();
//        return allStored.stream().map(x -> (K) x.getKey()).collect(Collectors.toSet());
//    }
//
//    public int size() {
//        return (int) datastore.getCount(getCollectionName());
//    }
//
//    public Set<Map.Entry<K, V>> getEntrySet() {
//        List<StoredEntity> allData = datastore.find(getCollectionName(), StoredEntity.class).asList();
//        return allData.stream().map(x -> {
//            Map.Entry<K, V> entry = new AbstractMap.SimpleEntry(x.getKey(), x.getValue());
//            return entry;
//        }).collect(Collectors.toSet());
//    }
//
//
//}
