import com.hazelcast.core.IMap;
import io.distmap.DistributedMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * Created by mich8bsp on 15-Jan-16.
 */
public class SimpleCommunicationTest {

    private static final String TEST_MAP = "TEST_MAP";
    private static IMap<String, String> map;

    @BeforeClass
    public static void before() throws InterruptedException {
        Thread.sleep(1000);
    }

    @Test
    public void testSendReceive() throws InterruptedException {
        map = new DistributedMap.MapBuilder<String, String>(TEST_MAP).setPartition("aaa").build();
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
        Assert.assertEquals(res,"dfs");

    }


}
