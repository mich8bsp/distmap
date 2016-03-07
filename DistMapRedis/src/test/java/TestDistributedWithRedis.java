import io.distmap.redis.DistributedMap;
import io.distmap.redis.ICallback;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.redisson.api.RMapReactive;
import org.redisson.core.RMap;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * Created by mich8bsp on 06-Mar-16.
 */
public class TestDistributedWithRedis {

    public static TestEntity key;
    public static TestEntity value;

    @BeforeClass
    public static void before() {
        key = new TestEntity(34, null, "");
        value = new TestEntity(34, Arrays.asList(3, 4, 5), "Entity1");
    }

    @Test
    public void test() throws InterruptedException {
        DistributedMap<Integer, String> map = new DistributedMap<>("TEST", 0);
        Map<Integer, String> testMap = map.<Integer, String>setPartition("dfs").build();
        testMap.put(4, "dfsd");
        Thread.sleep(3000);
        Assert.assertEquals("dfsd", testMap.get(4));

        DistributedMap<TestEntity, TestEntity> complexMap = new DistributedMap<>("TEST-2", 0);
        Map<TestEntity, TestEntity> testMap2 = complexMap.build();

        testMap2.put(key, value);
        Thread.sleep(3000);
        Assert.assertEquals(value, testMap2.get(key));


    }

    @Test
    public void testCallback() throws InterruptedException {
        DistributedMap<TestEntity, TestEntity> publisher = new DistributedMap<>("TEST-3", 0);
        Map<TestEntity, TestEntity> testMap3 = publisher.build();

        DistributedMap<TestEntity, TestEntity> subscriber = new DistributedMap<TestEntity, TestEntity>("TEST-3", 0).<TestEntity, TestEntity>addListener(new ICallback<TestEntity, TestEntity>() {
            @Override
            public void onDataArrival(TestEntity key, TestEntity value) {
                System.out.println("Got arrival " + key + " " + value);
            }

            @Override
            public void onDataRemoval(TestEntity key, TestEntity value) {
                System.out.println("Got removal " + key + " " + value);
            }
        });
        Map<TestEntity, TestEntity> testMap4 = subscriber.build();

        Thread.sleep(100);
        testMap3.put(key, value);
        Thread.sleep(1000);
        testMap3.remove(key);
        Thread.sleep(100);
        testMap3.put(key, value);
        Thread.sleep(1000);
        testMap3.clear();
        while (true){
            Thread.sleep(1000);
        }
    }
}
