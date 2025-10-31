import com.qiu.cache.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CacheTest {

    @Test
    public void testCacheEntry() {
        byte[] data = "test data".getBytes();
        CacheEntry entry = new CacheEntry("key1", data);

        assertEquals("key1", entry.getKey());
        assertArrayEquals(data, entry.getData());
        assertEquals(data.length, entry.getSize());
        assertEquals(1, entry.getAccessCount());

        // 测试防御性拷贝
        assertNotSame(data, entry.getData());

        // 测试访问记录
        entry.recordAccess();
        assertEquals(2, entry.getAccessCount());
    }

    @Test
    public void testLRUCacheBasicOperations() {
        LRUCache cache = new LRUCache(1024);

        // 测试插入和查找
        CacheHandle handle1 = cache.put("key1", "value1".getBytes());
        assertNotNull(handle1);
        assertEquals(1, cache.getEntryCount());

        CacheHandle handle2 = cache.get("key1");
        assertNotNull(handle2);
        assertArrayEquals("value1".getBytes(), handle2.getData());

        // 测试删除
        assertTrue(cache.delete("key1"));
        assertNull(cache.get("key1"));
        assertEquals(0, cache.getEntryCount());
    }

    @Test
    public void testLRUCacheEviction() {
        LRUCache cache = new LRUCache(100); // 小容量缓存

        // 插入多个条目，触发淘汰
        cache.put("key1", new byte[40]); // 40字节
        cache.put("key2", new byte[40]); // 40字节
        cache.put("key3", new byte[40]); // 40字节 - 应该触发淘汰

        // 由于容量限制，可能只有2个条目能保留
        assertTrue(cache.getEntryCount() <= 2);
        assertTrue(cache.getSize() <= 100);
    }

    @Test
    public void testLRUCacheAccessOrder() {
        LRUCache cache = new LRUCache(200);

        cache.put("key1", new byte[50]);
        cache.put("key2", new byte[50]);
        cache.put("key3", new byte[50]);

        // 访问key1，使其成为最近访问的
        cache.get("key1");

        // 插入新条目，应该淘汰key2（最旧的）
        cache.put("key4", new byte[50]);

        // key2应该被淘汰，key1应该还在
        assertNotNull(cache.get("key1"));
        assertNull(cache.get("key2")); // 可能被淘汰
        assertNotNull(cache.get("key3"));
        assertNotNull(cache.get("key4"));
    }

    @Test
    public void testCacheHandle() {
        LRUCache cache = new LRUCache(1024);
        CacheHandle handle = cache.put("key1", "value1".getBytes());

        assertNotNull(handle);
        assertEquals("key1", handle.getKey());
        assertArrayEquals("value1".getBytes(), handle.getData());

        // 测试句柄关闭
        assertFalse(handle.isClosed());
        handle.close();
        assertTrue(handle.isClosed());

        // 关闭后访问应该抛异常
        try {
            handle.getData();
            fail("Should throw exception after close");
        } catch (IllegalStateException e) {
            // 预期异常
        }
    }

    @Test
    public void testShardedCache() {
        ShardedCache cache = new ShardedCache(1024, 4);

        // 测试基本操作
        cache.put("key1", "value1".getBytes());
        cache.put("key2", "value2".getBytes());
        cache.put("key3", "value3".getBytes());

        assertNotNull(cache.get("key1"));
        assertNotNull(cache.get("key2"));
        assertNotNull(cache.get("key3"));

        assertEquals(3, cache.getEntryCount());
        assertEquals(4, cache.getShardCount());
    }

    @Test
    public void testCacheStats() {
        LRUCache cache = new LRUCache(1024);
        CacheStats stats = cache.getStats();

        // 初始状态
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0.0, stats.getHitRate(), 0.001);

        // 插入和查找
        cache.put("key1", "value1".getBytes());
        cache.get("key1"); // 命中
        cache.get("key2"); // 未命中

        stats = cache.getStats();
        assertEquals(1, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(0.5, stats.getHitRate(), 0.001);
    }

    @Test
    public void testCachePrune() {
        LRUCache cache = new LRUCache(200);

        // 填充缓存
        cache.put("key1", new byte[80]);
        cache.put("key2", new byte[80]);
        cache.put("key3", new byte[80]);

        // 修剪到较小容量
        cache.pruneTo(100);

        assertTrue(cache.getSize() <= 100);
        assertTrue(cache.getEntryCount() <= 2);
    }

    @Test
    public void testCacheClear() {
        LRUCache cache = new LRUCache(1024);

        cache.put("key1", "value1".getBytes());
        cache.put("key2", "value2".getBytes());

        assertEquals(2, cache.getEntryCount());

        cache.clear();

        assertEquals(0, cache.getEntryCount());
        assertEquals(0, cache.getSize());
        assertNull(cache.get("key1"));
        assertNull(cache.get("key2"));
    }

    @Test
    public void testCacheCapacity() {
        long capacity = 512;
        LRUCache cache = new LRUCache(capacity);

        assertEquals(capacity, cache.getCapacity());

        // 测试不会超过容量
        for (int i = 0; i < 100; i++) {
            cache.put("key" + i, new byte[100]); // 每个100字节
        }

        assertTrue(cache.getSize() <= capacity);
        assertTrue(cache.getUsageRatio() <= 1.0);
    }
}
