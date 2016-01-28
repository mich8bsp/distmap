import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import io.distmap.DistributedMap;
import io.distmap.MapCallback;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * Test for simple communication using synchronous and asynchronous messaging
 * Created by mich8bsp on 15-Jan-16.
 */
public class SimpleCommunicationTest {

    private static final String TEST_MAP = "TEST_MAP";
    private static IMap<String, String> map;
    private static final String DEFAULT_PARTITION = "aaa";

    private String result;

    private static int domain1 = 0;

    @BeforeClass
    public static void before() throws InterruptedException {
        map = buildMap(domain1, DEFAULT_PARTITION);
        Thread.sleep(1000);
    }

    private static IMap<String, String> buildMap(int domain, String partition) {
        return new DistributedMap<String, String>().new MapBuilder(TEST_MAP, domain).setPartition(partition).build();
    }

    @Test
    public void testSendReceive() throws InterruptedException {
        System.out.println("Putting at time " + System.currentTimeMillis());
        map.put("test", "dfs");
        IMap<String, String> map2 = buildMap(domain1, "bbb");
        Thread.sleep(500);
        System.out.println("Putting at time " + System.currentTimeMillis());
        map.put("test", "dfs");

        String res = map2.get("test");
        Assert.assertNull(res);
        res = map.get("test");
        Assert.assertNotNull(res);
        Assert.assertEquals(res, "dfs");

    }

    @Test
    public void testListener() throws InterruptedException {
        String key = "test";
        String oldValue = "dfs";
        map.put(key, oldValue);
        new DistributedMap<String, String>().new MapBuilder(TEST_MAP, domain1).setPartition(DEFAULT_PARTITION).setListener(new MapCallback<String, String>() {

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                result = event.getValue();
            }
        }).build();

        Assert.assertEquals(oldValue, map.get(key));
        Assert.assertNull(result);
        String newData = "abcde";
        map.put(key, newData);

        Thread.sleep(500);
        Assert.assertNotNull(result);
        Assert.assertEquals(newData, result);

    }

    @Test
    public void testListenerFilter() throws InterruptedException {
        String key = "test";
        String oldValue = "dfs";
        map.put(key, oldValue);
        new DistributedMap<String, String>().new MapBuilder(TEST_MAP, domain1).setPartition(DEFAULT_PARTITION).setListener(new MapCallback<String, String>() {

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                result = event.getValue();
            }
        }, (p -> p.getValue().contains("a"))).build();

        Assert.assertEquals(oldValue, map.get(key));
        Assert.assertNull(result);
        String newData = "abcde";
        map.put(key, newData);

        Thread.sleep(500);
        Assert.assertNotNull(result);
        Assert.assertEquals(newData, result);
        map.put(key, "shhhjhd");
        Thread.sleep(500);
        //was filtered
        Assert.assertEquals(newData, result);

    }

    @Test
    public void testDomainSeparation() {
        int domain2 = 2;
        Map<String, String> map2 = buildMap(domain2, DEFAULT_PARTITION);
        Map<String, String> map3 = buildMap(domain1, DEFAULT_PARTITION);
        String key = "key";
        String value = "dsfd";
        map.put(key, value);

        String resFromAnotherDomain = map2.get(key);
        String resFromSameDomain = map3.get(key);

        Assert.assertNull(resFromAnotherDomain);
        Assert.assertEquals(value, resFromSameDomain);
    }

//    @Test
//    public void manualTest() throws InterruptedException {
//        int count = 1;
//        while(true){
//            map.put("dsfs", "dfsa" + count++);
//            Thread.sleep(1000);
//        }
//    }
//
//
//    @Test
//    public void manualTest2() throws InterruptedException {
//        IMap<String, String> mapWithListener = new DistributedMap.MapBuilder<String, String>(TEST_MAP).setPartition(DEFAULT_PARTITION).setListener(new EntryUpdatedListener<String, String>() {
//
//            public void entryUpdated(EntryEvent<String, String> event) {
//                System.out.println("got result " + event.getValue());
//            }
//        }).build();
//        while(true){
//            Thread.sleep(1000);
//        }
//    }


}
