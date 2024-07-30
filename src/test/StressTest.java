import org.junit.Before;
import org.junit.Test;
import service.NormalStore;

import java.io.IOException;

import static org.junit.Assert.*;

public class StressTest {
    private NormalStore store;
    @Before
    public void setUp() throws Exception {
        // 初始化 NormalStore 实例
        store = new NormalStore();
    }

    @Test
    public void test() throws IOException {
        for (int i = 0; i < 1000; i++){
            store.set("fzx" + i,"" + i);
            System.out.println("set " + i);
        }

        store.close(); //触发落盘操作

        for (int i = 500; i < 750; i++){
            store.rm("fzx" + i);
            System.out.println("rm " + i);
        }

        store.close();

        for (int i = 750; i < 1500; i++) {
            store.set("fzx" + i, "" + i);
            System.out.println("set " + i);
        }

        store.close();

        for (int i = 1500; i < 2500; i++) {
            store.set("fzx" + i, "" + i);
            System.out.println("set " + i);
        }

        assertEquals("950", store.get("fzx" + 950));
        assertEquals("750", store.get("fzx" + 750));
        assertNull(store.get("fzx" + 749));
    }

    @Test
    public void test2() throws IOException {
        for (int i = 0;i < 100000;i++){
            store.set("test" + i,i + "");
            System.out.println("set " + i);
        }

        for (int i = 0;i < 100000;i++){
            assertEquals(i + "", store.get("test" + i));
            System.out.println(i);
        }
    }
}
