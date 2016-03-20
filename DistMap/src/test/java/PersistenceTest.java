import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import io.distmap.MapCallback;
import io.distmap.persistent.AbstractMapStore;
import io.distmap.persistent.DBInfo;
import io.distmap.persistent.PersistentDistributedMap;
import io.distmap.persistent.vertx.VertxMongoMapStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mich8bsp on 04-Mar-16.
 */
public class PersistenceTest {

    private static final String TEST_MAP = "TEST_MAP";
    private static final String DEFAULT_PARTITION = "aaa";
    private static AbstractMapStore<TestType, TestType> mapStore;
    private static DBInfo dbInfo;
    private TestType result;
    private static Comparator<TestType> sorter = (o1, o2) -> o1.getId() - o2.getId();

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

    @Before
    public void cleanup() {
        Map<TestType, TestType> map1 = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, 34, mapStore, dbInfo).setDirectToDB(true).setPartition(DEFAULT_PARTITION).build();
        Map<TestType, TestType> map2 = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, 35, mapStore, dbInfo).setDirectToDB(true).setPartition(DEFAULT_PARTITION).build();

        map1.clear();
        map2.clear();
    }

    // @Ignore
    @Test
    public void persistenceTest() throws InterruptedException {
        int domain = 34;
        Map<TestType, TestType> map1 = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, domain, mapStore, dbInfo).setPartition(DEFAULT_PARTITION).build();
        Map<TestType, TestType> map2 = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, domain, mapStore, dbInfo).setPartition(DEFAULT_PARTITION).setListener(new MapCallback<TestType, TestType>() {
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

        Map<TestType, TestType> map3 = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, domain, mapStore, dbInfo).setPartition(DEFAULT_PARTITION).build();
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
        result = null;
        long time = System.currentTimeMillis();
        while (result == null && (System.currentTimeMillis() - time) < 5000) {
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

    @Test
    public void testEmpty() {
        int domain = 35;
        Map<TestType, TestType> mapDirect = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, domain, mapStore, dbInfo).setDirectToDB(true).setPartition(DEFAULT_PARTITION).build();
        mapDirect.clear();
        TestType item2 = new TestType();
        item2.setId(2343);
        item2.setName("test");
        Assert.assertNull(mapDirect.remove(item2));
        Assert.assertFalse(mapDirect.containsKey(item2));
        Assert.assertFalse(mapDirect.containsValue(item2));
        Assert.assertTrue(mapDirect.entrySet().isEmpty());
        Assert.assertNull(mapDirect.get(item2));
        Assert.assertTrue(mapDirect.isEmpty());
        Assert.assertEquals(0, mapDirect.size());
        Assert.assertTrue(mapDirect.keySet().isEmpty());
        Assert.assertTrue(mapDirect.values().isEmpty());

    }

    @Test
    public void testMassiveRandom() {
        List<TestType> items = getRandomItems();
        int domain = 35;
        Map<TestType, TestType> mapDirect = new PersistentDistributedMap.PersistentMapBuilder<>(TEST_MAP, domain, mapStore, dbInfo).setDirectToDB(true).setPartition(DEFAULT_PARTITION).build();
        items.forEach(item -> {
            for (int i = 0; i < 5; i++) {
                mapDirect.put(cleanKey(item), item);
            }
        });
        Assert.assertEquals(items.size(), mapDirect.size());
        Collection<TestType> values = mapDirect.values();
        List<TestType> sortedValues = values.stream().sorted(sorter).collect(Collectors.toList());
        Assert.assertEquals(items, sortedValues);

        Set<TestType> keys = mapDirect.keySet();
        keys = keys.stream().sorted(sorter).collect(Collectors.toSet());
        Assert.assertEquals(items.stream().map(this::cleanKey).collect(Collectors.toSet()), keys);

        Set<Map.Entry<TestType, TestType>> entries = mapDirect.entrySet();
        Assert.assertEquals(items.stream().map(x -> new AbstractMap.SimpleEntry<>(cleanKey(x), x)).collect(Collectors.toSet()), entries);
        mapDirect.clear();
        Assert.assertEquals(0, mapDirect.keySet().size());
        Assert.assertEquals(0, mapDirect.values().size());
        Assert.assertEquals(0, mapDirect.entrySet().size());
    }

    private TestType cleanKey(TestType item) {
        TestType key = new TestType();
        key.setId(item.getId());
        key.setName("dsasd");
        key.setListSomething(Arrays.asList(3, 4, 5, 2, 4, 6, 2));
        key.setSomething(new String[]{"fkgklsdl"});
        return key;
    }

    private static List<TestType> getRandomItems() {
        int count = (int) (Math.random() * 1000 + 1);
        List<TestType> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TestType item = new TestType();
            item.setId(i + 1);
            item.setName(UUID.randomUUID().toString());
            int listSize = Math.abs(new Random().nextInt(100)) + 1;
            item.setListSomething(new LinkedList<>());
            String[] smth = new String[listSize];
            for (int j = 0; j < listSize; j++) {
                item.getListSomething().add(new Random().nextInt());
                smth[j] = UUID.randomUUID().toString();
            }
            item.setSomething(smth);
            items.add(item);
        }
        return items;
    }
}
