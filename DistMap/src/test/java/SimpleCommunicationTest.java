import io.distmap.EMapPermissions;
import io.distmap.NetAPI;
import org.junit.Test;

import java.util.Calendar;
import java.util.Map;

/**
 * Created by mich8bsp on 15-Jan-16.
 */
public class SimpleCommunicationTest {

    @Test
    public void testSend() throws InterruptedException {
        NetAPI netAPI = new NetAPI(3, "oper");
        Map<String, Calendar> map = netAPI.getDistributedMap("TEST-MAP", EMapPermissions.READ_WRITE);
        Calendar cal = Calendar.getInstance();
        cal.set(1991, Calendar.FEBRUARY, 20);
        map.put("test", cal);

        Thread.sleep(100000);
    }

    @Test
    public void testReceive() throws InterruptedException {
        NetAPI netAPI = new NetAPI(3, "oper");
        Map<String, Calendar> map = netAPI.getDistributedMap("TEST-MAP", EMapPermissions.READ_WRITE);
        while (true) {
            map.get("test");

            Thread.sleep(1000);
        }
    }
}
