import org.junit.Before;
import org.junit.Test;
import service.NormalStore;

import static org.junit.Assert.*;

public class RedologTest {
    private NormalStore store;
    @Before
    public void setUp() throws Exception {
        // 初始化 NormalStore 实例
        store = new NormalStore();
    }
    @Test
    public void testRedolog1(){
        for (int i = 0; i < 100; i++){
            store.set("test" + i,i + "");
        }
        throw new RuntimeException(); // 模拟崩溃, 此时数据还未落盘
    }

    @Test
    public void testRedolog2(){
        String test50 = store.get("test50");
        assertEquals("50",test50);
    }
}