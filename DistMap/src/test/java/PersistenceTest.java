import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import io.distmap.MapCallback;
import io.distmap.persistent.AbstractMapStore;
import io.distmap.persistent.DBInfo;
import io.distmap.persistent.PersistentDistributedMap;
import io.distmap.persistent.vertx.VertxMongoMapStore;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Created by mich8bsp on 04-Mar-16.
 */
public class PersistenceTest {

    private static final String TEST_MAP = "TEST_MAP";
    private static final String DEFAULT_PARTITION = "aaa";
    private static AbstractMapStore<TestType, TestType> mapStore;
    private static DBInfo dbInfo;
    private TestType result;

    @BeforeClass
    public static void beforeInit() {
        mapStore = new VertxMongoMapStore<TestType, TestType>() {
            @Override
            public Class<TestType> getStoredValueClass() {
                return TestType.class;
            }

            @Override
            public Class<TestType> getStoredKeyClass() {
                return TestType.class;
            }
        };
        dbInfo = new DBInfo("test-db", Collections.singletonList("localhost"), "guest", "guest");
    }


    @Ignore
    @Test
    public void persistenceTest() throws InterruptedException {
        int domain = 34;
        Map<TestType, TestType> map1 = new PersistentDistributedMap.PersistentMapBuilder<TestType, TestType>(TEST_MAP, domain, mapStore, dbInfo).setPartition(DEFAULT_PARTITION).build();
        Map<TestType, TestType> map2 = new PersistentDistributedMap.PersistentMapBuilder<TestType, TestType>(TEST_MAP, domain, mapStore, dbInfo).setPartition(DEFAULT_PARTITION).setListener(new MapCallback<TestType, TestType>() {
            @Override
            public void entryAdded(EntryEvent<TestType, TestType> event) {
                result = event.getValue();
            }
        }).build();
        TestType item = new TestType();
        item.setId(234);
        item.setName("test-name");
        map1.put(item, item);

        Thread.sleep(5000);
        Assert.assertNotNull(result);
        Assert.assertEquals(item, result);

        result = null;
        ((IMap) map1).destroy();
        ((IMap) map2).destroy();

        Map<TestType, TestType> map3 = new PersistentDistributedMap.PersistentMapBuilder<TestType, TestType>(TEST_MAP, domain, mapStore, dbInfo).setPartition(DEFAULT_PARTITION).build();
        Thread.sleep(1000);
        TestType fromDB = map3.get(item);
        Assert.assertNotNull(fromDB);
        Assert.assertEquals(item, fromDB);

    }

    @Test
    public void testDirectToDB() throws InterruptedException {
        int domain = 35;
        Map<TestType, TestType> mapDirect = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, domain, mapStore, dbInfo).setDirectToDB(true).setPartition(DEFAULT_PARTITION).build();
        TestType item = new TestType();
        item.setId(2341);
        item.setName("test-name");
        mapDirect.put(item, item);

        Thread.sleep(5000);
        TestType item2 = new TestType();
        item2.setId(2341);
        item2.setName("test");
        result=null;
        while(result==null) {
            result = mapDirect.get(item2);
            Thread.sleep(1000);
        }
        Assert.assertNotNull(result);
        Assert.assertEquals(item, result);

        Assert.assertFalse(mapDirect.isEmpty());

        TestType removed = mapDirect.remove(item2);
        Assert.assertNotNull(removed);
        Assert.assertEquals(result, removed);
        Assert.assertTrue(mapDirect.isEmpty());
    }
}
