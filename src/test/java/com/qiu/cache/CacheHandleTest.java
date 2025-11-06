package com.qiu.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.mockito.Mockito;
import static org.mockito.Mockito.*;

class CacheHandleTest {

    @Test
    void testConstructorAndBasicOperations() {
        CacheEntry entry = new CacheEntry("key", "data".getBytes());
        Cache cache = mock(Cache.class);

        CacheHandle handle = new CacheHandle(entry, cache);

        assertFalse(handle.isClosed());
        assertEquals(entry, handle.getEntry());
        assertEquals("key", handle.getKey());
        assertArrayEquals("data".getBytes(), handle.getData());
        assertEquals("data".getBytes().length, handle.getSize());

        // 验证访问被记录
        assertEquals(2, entry.getAccessCount()); // 构造函数中记录了一次
    }

    @Test
    void testConstructorWithNullParameters() {
        CacheEntry entry = new CacheEntry("key", "data".getBytes());
        Cache cache = mock(Cache.class);

        assertThrows(NullPointerException.class, () -> new CacheHandle(null, cache));
        assertThrows(NullPointerException.class, () -> new CacheHandle(entry, null));
    }

    @Test
    void testClose() {
        CacheEntry entry = new CacheEntry("key", "data".getBytes());
        Cache cache = mock(Cache.class);

        CacheHandle handle = new CacheHandle(entry, cache);

        handle.close();

        assertTrue(handle.isClosed());
        verify(cache, times(1)).releaseHandle(handle);

        // 重复关闭应该没有效果
        handle.close();
        verify(cache, times(1)).releaseHandle(handle); // 仍然只调用一次
    }

    @Test
    void testOperationsAfterCloseThrowException() {
        CacheEntry entry = new CacheEntry("key", "data".getBytes());
        Cache cache = mock(Cache.class);

        CacheHandle handle = new CacheHandle(entry, cache);
        handle.close();

        assertThrows(IllegalStateException.class, handle::getEntry);
        assertThrows(IllegalStateException.class, handle::getKey);
        assertThrows(IllegalStateException.class, handle::getData);
        assertThrows(IllegalStateException.class, handle::getSize);
    }

    @Test
    void testAutoCloseable() throws Exception {
        CacheEntry entry = new CacheEntry("key", "data".getBytes());
        Cache cache = mock(Cache.class);

        try (CacheHandle handle = new CacheHandle(entry, cache)) {
            assertFalse(handle.isClosed());
            assertEquals("key", handle.getKey());
        }

        // 在 try-with-resources 块结束后应该自动关闭
        // 这里我们无法直接验证，但可以确认缓存被通知
        verify(cache, times(1)).releaseHandle(any());
    }

    @Test
    void testDataDefensiveCopy() {
        byte[] originalData = "original".getBytes();
        CacheEntry entry = new CacheEntry("key", originalData);
        Cache cache = mock(Cache.class);

        CacheHandle handle = new CacheHandle(entry, cache);

        byte[] handleData = handle.getData();
        handleData[0] = 'X';

        // 修改返回的数据不应影响原始数据
        assertNotEquals('X', handle.getData()[0]);
    }
}