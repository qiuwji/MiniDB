package com.qiu.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

class CacheEntryTest {

    @Test
    void testConstructorAndGetters() {
        String key = "testKey";
        byte[] data = "testData".getBytes();

        CacheEntry entry = new CacheEntry(key, data);

        assertEquals(key, entry.getKey());
        assertArrayEquals(data, entry.getData());
        assertEquals(data.length, entry.getSize());
        assertTrue(entry.getCreateTime() > 0);
        assertEquals(entry.getCreateTime(), entry.getLastAccessTime());
        assertEquals(1, entry.getAccessCount());
    }

    @Test
    void testConstructorWithNullKey() {
        byte[] data = "test".getBytes();
        assertThrows(NullPointerException.class, () -> new CacheEntry(null, data));
    }

    @Test
    void testConstructorWithNullData() {
        assertThrows(NullPointerException.class, () -> new CacheEntry("key", null));
    }

    @Test
    void testRecordAccess() throws InterruptedException {
        byte[] data = "data".getBytes();
        CacheEntry entry = new CacheEntry("key", data);

        long firstAccessTime = entry.getLastAccessTime();
        int firstAccessCount = entry.getAccessCount();

        Thread.sleep(10); // 确保时间不同

        entry.recordAccess();

        assertEquals(firstAccessCount + 1, entry.getAccessCount());
        assertTrue(entry.getLastAccessTime() > firstAccessTime);
    }

    @Test
    void testDataDefensiveCopy() {
        byte[] originalData = "original".getBytes();
        CacheEntry entry = new CacheEntry("key", originalData);

        // 修改原始数组不应影响缓存条目
        originalData[0] = 'X';

        assertNotEquals('X', entry.getData()[0]);
        assertArrayEquals("original".getBytes(), entry.getData());
    }

    @Test
    void testGetDataReturnsCopy() {
        byte[] data = "test".getBytes();
        CacheEntry entry = new CacheEntry("key", data);

        byte[] returnedData = entry.getData();
        returnedData[0] = 'X';

        // 修改返回的数组不应影响内部数据
        assertNotEquals('X', entry.getData()[0]);
    }

    @Test
    void testEqualsAndHashCode() {
        byte[] data1 = "data".getBytes();
        byte[] data2 = "data".getBytes();
        byte[] differentData = "different".getBytes();

        CacheEntry entry1 = new CacheEntry("key", data1);
        CacheEntry entry2 = new CacheEntry("key", data2);
        CacheEntry entry3 = new CacheEntry("key", differentData);
        CacheEntry entry4 = new CacheEntry("differentKey", data1);

        assertEquals(entry1, entry2);
        assertNotEquals(entry1, entry3);
        assertNotEquals(entry1, entry4);
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void testToString() {
        CacheEntry entry = new CacheEntry("testKey", "testData".getBytes());
        String str = entry.toString();

        assertTrue(str.contains("testKey"));
        assertTrue(str.contains("size="));
        assertTrue(str.contains("accesses="));
    }
}