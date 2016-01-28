import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import io.distmap.DistributedMap;
import io.distmap.MapCallback;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for simple communication using synchronous and asynchronous messaging
 * Created by mich8bsp on 15-Jan-16.
 */
public class SimpleCommunicationTest {

    private static final String TEST_MAP = "TEST_MAP";
    private static IMap<String, String> map;
    private static final String DEFAULT_PARTITION = "aaa";

    private String result;

    @BeforeClass
    public static void before() throws InterruptedException {
        map = new DistributedMap.MapBuilder<String, String>(TEST_MAP).setPartition(DEFAULT_PARTITION).build();
        Thread.sleep(1000);
    }

    @Test
    public void testSendReceive() throws InterruptedException {
        System.out.println("Putting at time " + System.currentTimeMillis());
        map.put("test", "dfs");
        IMap<String, String> map2 = new DistributedMap.MapBuilder<String, String>(TEST_MAP).setPartition("bbb").build();
        Thread.sleep(5000);
        System.out.println("Putting at time " + System.currentTimeMillis());
        map.put("test", "dfs");

        String res = map2.get("test");

        Assert.assertNull(res);
        Thread.sleep(500);

        res = map.get("test");
        Assert.assertNotNull(res);
        Assert.assertEquals(res, "dfs");

    }

    @Test
    public void testListener() throws InterruptedException {
        String key = "test";
        String oldValue = "dfs";
        map.put(key, oldValue);
        new DistributedMap.MapBuilder<String, String>(TEST_MAP).setPartition(DEFAULT_PARTITION).setListener(new MapCallback<String, String>() {

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                result = event.getValue();
            }
        }).build();

        Assert.assertEquals(oldValue, map.get(key));
        Assert.assertNull(result);
        String newData = "abcde";
        map.put(key, newData);

        Thread.sleep(1000);
        Assert.assertNotNull(result);
        Assert.assertEquals(newData, result);

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
