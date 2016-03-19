package io.distmap.persistent.vertx;

import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import io.distmap.persistent.AbstractMapStore;
import io.distmap.persistent.DBInfo;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public abstract class VertxMongoMapStore<K extends Keyed, V> extends AbstractMapStore<K, V> {

    private static final int LIMIT = 2;
    private static final String TIMESTAMP = "timestamp";
    private static final long POLL_TIMEOUT = 5000;
    private MongoClient client;

    private static JsonObject sorter = new JsonObject().put("sort", TIMESTAMP);

    @Override
    public void connectToDB(DBInfo dbInfo) {
        JsonObject config = getConfig(dbInfo);
        client = MongoClient.createShared(VertxUtils.getInstance(), config);

    }

    protected static JsonObject getConfig(DBInfo dbInfo) {
        return new JsonObject();
    }

    @Override
    public void store(K key, V value) {
        try {
            JsonObject keyQuery = buildKeyQuery(key);
            JsonObject jsonValue = SerializationHelper.saveObjectToJson(value);
            if (keyQuery == null || jsonValue == null) {
                //log errors
                return;
            }
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

        client.findWithOptions(getCollectionName(), keyQuery, new FindOptions().setSort(sorter), res -> {
            if (res.succeeded()) {
                List<JsonObject> historicalValues = res.result();
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
            List<String> keyFields = Arrays.asList(key.getKeyFields()).stream().map(Field::getName).collect(Collectors.toList());
            keyJson.fieldNames().stream().filter(field -> !keyFields.contains(field)).forEach(keyQuery::remove);
            return keyQuery;
        } else {
            return null;
        }
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
            client.findWithOptions(getCollectionName(), keyQuery, new FindOptions().setSort(sorter), res -> {
                if (res.succeeded() && res.result().size() > 0) {
                    JsonObject result = res.result().get(res.result().size() - 1);
                    result.remove(TIMESTAMP);
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
