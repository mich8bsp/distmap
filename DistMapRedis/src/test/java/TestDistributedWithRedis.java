import io.distmap.redis.DistributedMap;
import io.distmap.redis.ICallback;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mich8bsp on 06-Mar-16.
 */
public class TestDistributedWithRedis {

    public static TestEntity key;
    public static TestEntity value;

    public static AtomicInteger arrivedCounter = new AtomicInteger(0);
    public static AtomicInteger removedCounter = new AtomicInteger(0);

    @BeforeClass
    public static void before() {
        key = new TestEntity(34, null, "");
        value = new TestEntity(34, Arrays.asList(3, 4, 5), "Entity1");
    }

    @Test
    public void testCommunication() throws InterruptedException {
        DistributedMap<Integer, String> map = new DistributedMap<>("TEST", 0);
        Map<Integer, String> testMap = map.<Integer, String>setPartition("dfs").build();
        testMap.put(4, "dfsd");
        Thread.sleep(100);
        Assert.assertEquals("dfsd", testMap.get(4));

        DistributedMap<TestEntity, TestEntity> complexMap = new DistributedMap<>("TEST-2", 0);
        Map<TestEntity, TestEntity> testMap2 = complexMap.build();

        testMap2.put(key, value);
        Thread.sleep(100);
        Assert.assertEquals(value, testMap2.get(key));


    }

    @Test
    public void testCallback() throws InterruptedException {
        DistributedMap<TestEntity, TestEntity> publisher = new DistributedMap<>("TEST-3", 0);
        Map<TestEntity, TestEntity> testMap3 = publisher.build();

        DistributedMap<TestEntity, TestEntity> subscriber = new DistributedMap<TestEntity, TestEntity>("TEST-3", 0).<TestEntity, TestEntity>addListener(new ICallback<TestEntity, TestEntity>() {
            @Override
            public void onDataArrival(TestEntity key, TestEntity value) {
                if (key.equals(TestDistributedWithRedis.key) && value.equals(TestDistributedWithRedis.value)) {
                    arrivedCounter.incrementAndGet();
                }
            }

            @Override
            public void onDataRemoval(TestEntity key, TestEntity value) {
                if (key.equals(TestDistributedWithRedis.key) && value.equals(TestDistributedWithRedis.value)) {
                    removedCounter.incrementAndGet();
                }
            }
        });
        subscriber.build();

        Thread.sleep(100);
        testMap3.put(key, value);
        Thread.sleep(100);
        testMap3.remove(key);
        Thread.sleep(100);
        testMap3.put(key, value);
        Thread.sleep(100);
        testMap3.clear();
        Thread.sleep(100);
        Assert.assertEquals(2, arrivedCounter.get());
        Assert.assertEquals(2, removedCounter.get());
    }

    @Test
    public void testPartitionSeparation() throws InterruptedException {
        Map<TestEntity, TestEntity> testMap = new DistributedMap<TestEntity, TestEntity>("TEST", 0).setPartition("partition1").build();
        Map<TestEntity, TestEntity> testMap2 = new DistributedMap<TestEntity, TestEntity>("TEST", 0).setPartition("partition2").build();
        Map<TestEntity, TestEntity> testMap3 = new DistributedMap<TestEntity, TestEntity>("TEST", 0).setPartition("partition2").build();

        testMap2.put(key, value);
        Thread.sleep(100);
        Assert.assertNull(testMap.get(key));
        Assert.assertEquals(value, testMap3.get(key));
    }
}
