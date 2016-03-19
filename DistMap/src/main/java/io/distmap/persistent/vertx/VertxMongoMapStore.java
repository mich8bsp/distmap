package io.distmap.persistent.vertx;

import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import io.distmap.persistent.AbstractMapStore;
import io.distmap.persistent.DBInfo;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public abstract class VertxMongoMapStore<K, V> extends AbstractMapStore<K, V> {

    //FIXME: make this configurable (2 is a sane default)
    private static final int LIMIT = 2;
    private static final String TIMESTAMP = "timestamp";
    private static final String MONGO_DOCUMENT_ID = "_id";
    //FIXME: make this configurable (need to think about a good default... 1 sec?)
    private static final long POLL_TIMEOUT = 5000;
    private MongoClient client;

    private static Comparator<JsonObject> sorter = (o1, o2) -> (int) (o1.getLong(TIMESTAMP) - o2.getLong(TIMESTAMP));

    @Override
    public void connectToDB(DBInfo dbInfo) {
        JsonObject config = getConfig(dbInfo);
        client = MongoClient.createShared(VertxUtils.getInstance(), config);

    }

    protected static JsonObject getConfig(DBInfo dbInfo) {
        JsonObject config = new JsonObject();
        config.put("host", dbInfo.getDbHosts().get(0));
        config.put("port", DEFAULT_MONGO_PORT);
        config.put("username", dbInfo.getUserName());
        config.put("password", dbInfo.getPassword());
        config.put("db_name", dbInfo.getDbName());
        return config;

    }

    @Override
    public void store(K key, V value) {
        requireNonNull(client, "Mongo client cannot be null");
        try {
            JsonObject keyQuery = buildKeyQuery(key);
            JsonObject jsonValue = SerializationHelper.saveObjectToJson(value);
            requireNonNull(keyQuery, "Key query cannot be null, all entities must be keyed");
            requireNonNull(jsonValue, "Failed to serialize value to json");
            jsonValue.put(TIMESTAMP, System.currentTimeMillis());
            client.save(getCollectionName(), jsonValue, res -> {
                if (res.succeeded()) {
                    ensureMaxSamplesPerInstance(keyQuery, LIMIT);
                }
            });
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private void ensureMaxSamplesPerInstance(JsonObject keyQuery, int limit) {
        requireNonNull(client, "Mongo client cannot be null");
        client.find(getCollectionName(), keyQuery, res -> {
            if (res.succeeded()) {
                List<JsonObject> historicalValues = res.result();
                historicalValues.sort(sorter);
                if (historicalValues.size() > limit && limit > 0) {
                    long lastRelevantTime = historicalValues.get(historicalValues.size() - limit + 1).getLong(TIMESTAMP);
                    DBObject builder = QueryBuilder.start().put(TIMESTAMP).lessThan(lastRelevantTime).get();
                    JsonObject removeQuery = new JsonObject(builder.toMap());
                    removeQuery.mergeIn(keyQuery);
                    client.remove(getCollectionName(), removeQuery, removeRes -> {

                    });
                }
            }
        });
    }

    private JsonObject buildKeyQuery(K key) throws IllegalAccessException {
        JsonObject keyJson = SerializationHelper.saveObjectToJson(key);
        if (keyJson != null) {
            JsonObject keyQuery = keyJson.copy();
            List<Field> keyFields = getKeyFields();
            if(keyFields==null || keyFields.isEmpty()){
                return null;
            }
            List<String> keyFieldNames = keyFields.stream().map(Field::getName).collect(Collectors.toList());
            keyJson.fieldNames().stream().filter(field -> !keyFieldNames.contains(field)).forEach(keyQuery::remove);
            return keyQuery;
        } else {
            return null;
        }
    }

    public List<Field> getKeyFields(){
        List<Field> keyFields = new LinkedList<>();
        for(Field field : getStoredKeyClass().getDeclaredFields()){
            if(field.isAnnotationPresent(Key.class)){
                keyFields.add(field);
            }
        }
        return keyFields;
    }

    @Override
    public void delete(K key) {
        try {
            JsonObject keyQuery = buildKeyQuery(key);
            client.remove(getCollectionName(), keyQuery, res -> {
            });
        } catch (IllegalAccessException e) {
        }
    }

    @Override
    public V load(K key) {
        try {
            BlockingQueue<JsonObject> resultQueue = new LinkedBlockingQueue<>(1);
            JsonObject keyQuery = buildKeyQuery(key);
            client.find(getCollectionName(), keyQuery, res -> {
                if (res.succeeded() && res.result().size() > 0) {
                    List<JsonObject> results = res.result();
                    results.sort(sorter);
                    JsonObject result = results.get(res.result().size() - 1);
                    result.remove(TIMESTAMP);
                    result.remove(MONGO_DOCUMENT_ID);
                    resultQueue.offer(result);
                }
            });
            JsonObject result = resultQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
            if (result != null) {
                return SerializationHelper.readObjectFromJson(result, getStoredValueClass());
            }
        } catch (IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        throw new UnsupportedOperationException();
    }

    public int size() {
        BlockingQueue<Integer> resQueue = new LinkedBlockingQueue<>(1);
        client.count(getCollectionName(), new JsonObject(), res -> {
            //log result
            if (res.succeeded()) {
                resQueue.offer(res.result().intValue());
            }
        });
        try {
            Integer size = resQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
            return (size == null) ? -1 : size;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public Set<Map.Entry<K, V>> getEntrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        if (client != null) {
            client.close();
        }
    }
}
