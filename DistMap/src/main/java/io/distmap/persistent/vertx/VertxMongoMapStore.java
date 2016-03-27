package io.distmap.persistent.vertx;

import io.distmap.persistent.AbstractMapStore;
import io.distmap.persistent.DBInfo;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public class VertxMongoMapStore<K, V> extends AbstractMapStore<K, V> {

    //FIXME: make this configurable (2 is a sane default)
    private static final int LIMIT = 2;
    private static final String TIMESTAMP = "timestamp";
    private static final String MONGO_DOCUMENT_ID = "_id";
    //FIXME: make this configurable (need to think about a good default... 1 sec?)
    private static final long POLL_TIMEOUT = 5000;
    private static final String KEY_ID = "keyId";
    private MongoClient client;

    private static Comparator<JsonObject> sorter = (o1, o2) -> (int) (o1.getLong(TIMESTAMP) - o2.getLong(TIMESTAMP));

    public VertxMongoMapStore(Map.Entry<Class<? extends K>, Class<? extends V>> typesEntry) {
        this.typesEntry = typesEntry;
    }

    @Override
    public void connectToDB(DBInfo dbInfo) {
        JsonObject config = getConfig(dbInfo);
        client = MongoClient.createShared(VertxUtils.getInstance(), config);
    }

    protected static JsonObject getConfig(DBInfo dbInfo) {
        JsonObject config = new JsonObject();
        String dbHost = dbInfo.getDbHosts().get(0);
        int port = DEFAULT_MONGO_PORT;
        if (dbHost.contains(":")) {
            String[] splitDbName = dbHost.split(":");
            dbHost = splitDbName[0];
            port = Integer.parseInt(splitDbName[1]);
        }
        config.put("host", dbHost);
        config.put("port", port);
        config.put("username", dbInfo.getUserName());
        config.put("password", dbInfo.getPassword());
        config.put("db_name", dbInfo.getDbName());
        return config;

    }

    @Override
    public void store(K key, V value) {
        this.store(key, value, false);
    }

    public void store(K key, V value, boolean async) {
        requireNonNull(client, "Mongo client cannot be null");
        try {
            JsonObject jsonValue = SerializationHelper.saveObjectToJson(value);
            requireNonNull(jsonValue, "Failed to serialize value to json");
            String keyId = getKeyId(key, true);
            if (keyId == null) {
                //saving key failed
                return;
            }
            jsonValue.put(TIMESTAMP, System.currentTimeMillis());
            jsonValue.put(KEY_ID, keyId);
            CountDownLatch latch = new CountDownLatch(1);
            if (LIMIT > 0) {
                client.find(getValuesCollectionName(), buildKeyIdQuery(keyId), res -> {
                    if (res.succeeded()) {
                        List<JsonObject> results = res.result();
                        if (results.size() >= LIMIT) {
                            results.sort(sorter);
                            JsonObject toReplace = results.get(0);
                            client.replace(getValuesCollectionName(), toReplace, jsonValue, replaceRes -> latch.countDown());
                        } else {
                            client.save(getValuesCollectionName(), jsonValue, saveRes -> latch.countDown());
                        }
                    }
                });
            }
            if (!async) {
                latch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
            }
        } catch (IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getKeyId(K key, boolean insertIfNotExist) throws IllegalAccessException, InterruptedException {
        JsonObject keyQuery = buildKeyFieldQuery(key);
        requireNonNull(keyQuery, "Key query cannot be null, all entities must be keyed");
        List<String> keyIdList = new ArrayList<>(1);
        CountDownLatch latch = new CountDownLatch(1);
        client.find(getKeysCollectionName(), keyQuery, res -> {
            if (res.succeeded() && res.result().size() > 0) {
                JsonObject storedKey = res.result().get(0);
                String keyId = storedKey.getString(MONGO_DOCUMENT_ID);
                keyIdList.add(keyId);
                latch.countDown();
            } else {
                if (insertIfNotExist) {
                    JsonObject keyAsJson = SerializationHelper.saveObjectToJson(key);
                    client.save(getKeysCollectionName(), keyAsJson, saveRes -> {
                        if (saveRes.succeeded()) {
                            String keyId = saveRes.result();
                            keyIdList.add(keyId);
                        }
                        latch.countDown();
                    });
                } else {
                    latch.countDown();
                }
            }
        });
        latch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
        if (keyIdList.isEmpty()) {
            return null;
        }
        return keyIdList.get(0);
    }

    private JsonObject buildKeyIdQuery(String keyId) {
        return new JsonObject().put(KEY_ID, keyId);
    }

    private JsonObject buildKeyFieldQuery(K key) throws IllegalAccessException {
        JsonObject keyJson = SerializationHelper.saveObjectToJson(key);
        if (keyJson != null) {
            JsonObject keyQuery = keyJson.copy();
            List<Field> keyFields = getKeyFields();
            if (keyFields == null || keyFields.isEmpty()) {
                return null;
            }
            List<String> keyFieldNames = keyFields.stream().map(Field::getName).collect(Collectors.toList());
            keyJson.fieldNames().stream().filter(field -> !keyFieldNames.contains(field)).forEach(keyQuery::remove);
            return keyQuery;
        } else {
            return null;
        }
    }

    public List<Field> getKeyFields() {
        List<Field> keyFields = new LinkedList<>();
        for (Field field : typesEntry.getKey().getDeclaredFields()) {
            if (field.isAnnotationPresent(Key.class)) {
                keyFields.add(field);
            }
        }
        return keyFields;
    }

    @Override
    public void delete(K key) {
        try {
            String keyId = getKeyId(key, false);
            if (keyId == null) {
                return;
            }
            JsonObject keyQuery = buildKeyFieldQuery(key);
            CountDownLatch latch = new CountDownLatch(2);
            client.remove(getKeysCollectionName(), keyQuery, res -> latch.countDown());
            client.remove(getValuesCollectionName(), buildKeyIdQuery(keyId), res -> latch.countDown());
            latch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public V load(K key) {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            String keyId = getKeyId(key, false);
            List<JsonObject> receivedResult = new ArrayList<>(1);
            client.find(getValuesCollectionName(), buildKeyIdQuery(keyId), res -> {
                if (res.succeeded() && res.result().size() > 0) {
                    List<JsonObject> results = res.result();
                    JsonObject result = results.stream().max(sorter).get();
                    processValueAfterRetrieving(result);
                    receivedResult.add(result);
                }
                latch.countDown();
            });
            latch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
            if (receivedResult.size() > 0) {
                JsonObject result = receivedResult.get(0);
                if (result != null) {
                    return SerializationHelper.readObjectFromJson(result, typesEntry.getValue());
                }
            }
        } catch (IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void processValueAfterRetrieving(JsonObject result) {
        result.remove(TIMESTAMP);
        result.remove(MONGO_DOCUMENT_ID);
        result.remove(KEY_ID);
    }

    private void processKeyAfterRetrieving(JsonObject result) {
        result.remove(MONGO_DOCUMENT_ID);
    }

    public int size() {
        AtomicInteger count = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);
        client.find(getValuesCollectionName(), new JsonObject(), res -> {
            //log result
            if (res.succeeded()) {
                count.set((int) res.result().stream().map(x -> x.getString(KEY_ID)).distinct().count());
            }
            latch.countDown();
        });
        try {
            latch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
            return count.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public Set<Map.Entry<K, V>> getEntrySet() {
        Iterable<K> keys = loadAllKeys();
        if (keys == null) {
            return null;
        }
        Set<Map.Entry<K, V>> entries = new HashSet<>();
        for (K key : keys) {
            Map.Entry<K, V> entry = new AbstractMap.SimpleEntry<>(key, load(key));
            entries.add(entry);
        }
        return entries;
    }


    @Override
    public Iterable<K> loadAllKeys() {
        AtomicInteger count = new AtomicInteger(-1);
        List<JsonObject> allResults = new LinkedList<>();
        CountDownLatch findLatch = new CountDownLatch(1);
        client.find(getKeysCollectionName(), new JsonObject(), res -> {
            if (res.succeeded()) {
                List<JsonObject> results = res.result();
                results.forEach(this::processKeyAfterRetrieving);
                count.set(results.size());
                allResults.addAll(results);
            }
            findLatch.countDown();
        });
        try {
            findLatch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
            if (count.get() == -1) {
                //failed
                return null;
            }
            if (count.get() == 0) {
                return new LinkedList<>();
            }
            return allResults.stream().map(x -> SerializationHelper.readObjectFromJson(x, typesEntry.getKey())).collect(Collectors.toList());
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deleteAll() {
        CountDownLatch latch = new CountDownLatch(2);
        client.dropCollection(getKeysCollectionName(), res -> latch.countDown());
        client.dropCollection(getValuesCollectionName(), res -> latch.countDown());
        //this is quite ugly but we want to ensure that the call blocks until all collections were dropped
        try {
            latch.await(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    @Override
    public boolean containsKey(K key) {
        String keyId = null;
        try {
            keyId = getKeyId(key, false);
        } catch (IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
        return keyId != null;
    }


    @Override
    public void destroy() {
        if (client != null) {
            client.close();
        }
    }
}
