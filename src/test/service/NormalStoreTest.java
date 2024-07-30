package service;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class NormalStoreTest {

    private NormalStore store;

    @Before
    public void setUp() throws Exception {
        // 初始化 NormalStore 实例
        store = new NormalStore();
    }

    @Test
    public void testSetAndGet() {
        // 设置键值对
        String key = "testKey";
        String value = "testValue";
        store.set(key, value);

        // 验证 get 方法返回预期的值
        String result = store.get(key);
        assertEquals("Value should match", value, result);
    }

    @Test
    public void testRemove() {
        // 先设置一个键值对
        store.set("rmKey", "rmValue");

        // 验证键存在
        assertTrue("Key should exist before removal", store.get("rmKey") != null);

        // 执行删除操作
        store.rm("rmKey");

        // 验证键不存在
        assertNull("Key should not exist after removal", store.get("rmKey"));
    }

    // 可以添加更多的测试方法来测试不同的功能
}