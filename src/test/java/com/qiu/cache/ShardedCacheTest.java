package com.qiu.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;

class ShardedCacheTest {

    private ShardedCache cache;
    private static final long CAPACITY = 1024;

    @BeforeEach
    void setUp() {
        cache = new ShardedCache(CAPACITY, 4); // 4个分片
    }

    @Test
    void testConstructor() {
        assertEquals(CAPACITY, cache.getCapacity());
        assertEquals(4, cache.getShardCount());

        // 测试非法参数
        assertThrows(IllegalArgumentException.class, () -> new ShardedCache(0));
        assertThrows(IllegalArgumentException.class, () -> new ShardedCache(CAPACITY, 0));
        assertThrows(IllegalArgumentException.class, () -> new ShardedCache(CAPACITY, 4, 0));
    }

    @Test
    void testPutAndGet() {
        String key = "testKey";
        byte[] value = "testValue".getBytes();

        CacheHandle handle = cache.put(key, value);
        assertNotNull(handle);

        try (CacheHandle retrieved = cache.get(key)) {
            assertNotNull(retrieved);
            assertEquals(key, retrieved.getKey());
            assertArrayEquals(value, retrieved.getData());
        }
    }

    @Test
    void testShardDistribution() {
        // 测试键是否均匀分布在分片中
        String[] keys = {"key1", "key2", "key3", "key4", "key5"};

        for (String key : keys) {
            cache.put(key, key.getBytes());
        }

        // 检查所有键都能正确检索
        for (String key : keys) {
            assertNotNull(cache.get(key));
        }
    }

    @Test
    void testDelete() {
        String key = "keyToDelete";
        byte[] value = "value".getBytes();

        cache.put(key, value);
        assertTrue(cache.delete(key));
        assertFalse(cache.delete("nonexistent"));
        assertNull(cache.get(key));
    }

    @Test
    void testClear() {
        cache.put("key1", "val1".getBytes());
        cache.put("key2", "val2".getBytes());

        cache.clear();

        assertEquals(0, cache.getSize());
        assertEquals(0, cache.getEntryCount());
        assertNull(cache.get("key1"));
        assertNull(cache.get("key2"));
    }

    @Test
    void testGetSizeAndEntryCount() {
        assertEquals(0, cache.getSize());
        assertEquals(0, cache.getEntryCount());

        cache.put("key1", "val1".getBytes());
        cache.put("key2", "val2".getBytes());

        assertTrue(cache.getSize() > 0);
        assertEquals(2, cache.getEntryCount());
    }

    @Test
    void testPruneTo() {
        // 填充一些数据
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, ("value" + i).getBytes());
        }

        long originalSize = cache.getSize();
        assertTrue(originalSize > 0);

        cache.pruneTo(originalSize / 2);

        assertTrue(cache.getSize() <= originalSize / 2);
    }

    @Test
    void testGetShardStats() {
        // 添加一些数据到特定分片
        String testKey = "testKey";
        cache.put(testKey, "value".getBytes());

        int shardIndex = cache.getShardIndex(testKey);
        CacheStats shardStats = cache.getShardStats(shardIndex);

        assertNotNull(shardStats);
        assertTrue(shardStats.getInsertCount() >= 1);

        // 测试无效分片索引
        assertThrows(IllegalArgumentException.class, () -> cache.getShardStats(-1));
        assertThrows(IllegalArgumentException.class, () -> cache.getShardStats(100));
    }

    @Test
    void testGetShardLoadFactors() {
        double[] loadFactors = cache.getShardLoadFactors();
        assertNotNull(loadFactors);
        assertEquals(4, loadFactors.length); // 4个分片

        for (double factor : loadFactors) {
            assertEquals(0.0, factor, 0.001);
        }

        // 添加数据后重新检查
        cache.put("key", "value".getBytes());
        loadFactors = cache.getShardLoadFactors();

        boolean hasNonZero = false;
        for (double factor : loadFactors) {
            if (factor > 0) {
                hasNonZero = true;
                break;
            }
        }
        assertTrue(hasNonZero);
    }

    @Test
    void testStatsAggregation() {
        // 添加数据到多个分片
        cache.put("key1", "val1".getBytes());
        cache.put("key2", "val2".getBytes());
        cache.put("key3", "val3".getBytes());

        CacheStats totalStats = cache.getStats();
        assertTrue(totalStats.getInsertCount() >= 3);
    }

    @Test
    void testToString() {
        String str = cache.toString();
        assertTrue(str.contains("ShardedCache"));
        assertTrue(str.contains("shards=4"));
    }
}