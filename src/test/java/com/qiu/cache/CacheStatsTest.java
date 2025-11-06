package com.qiu.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CacheStatsTest {

    @Test
    void testInitialState() {
        CacheStats stats = new CacheStats();

        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getInsertCount());
        assertEquals(0, stats.getDeleteCount());
        assertEquals(0, stats.getEvictionCount());
        assertEquals(0, stats.getRequestCount());
        assertEquals(0.0, stats.getHitRate());
        assertEquals(0.0, stats.getMissRate());
    }

    @Test
    void testRecordOperations() {
        CacheStats stats = new CacheStats();

        stats.recordHit();
        stats.recordHit();
        stats.recordMiss();
        stats.recordInsert();
        stats.recordDelete();
        stats.recordEviction();

        assertEquals(2, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(1, stats.getInsertCount());
        assertEquals(1, stats.getDeleteCount());
        assertEquals(1, stats.getEvictionCount());
        assertEquals(3, stats.getRequestCount());
    }

    @Test
    void testHitRateCalculation() {
        CacheStats stats = new CacheStats();

        // 没有请求时
        assertEquals(0.0, stats.getHitRate());

        // 只有命中
        stats.recordHit();
        stats.recordHit();
        assertEquals(1.0, stats.getHitRate());

        // 混合情况
        stats.recordMiss();
        assertEquals(2.0 / 3.0, stats.getHitRate(), 0.001);
    }

    @Test
    void testCopyConstructor() {
        CacheStats original = new CacheStats();
        original.recordHit();
        original.recordMiss();
        original.recordInsert();

        CacheStats copy = new CacheStats(original);

        assertEquals(original.getHitCount(), copy.getHitCount());
        assertEquals(original.getMissCount(), copy.getMissCount());
        assertEquals(original.getInsertCount(), copy.getInsertCount());

        // 修改原始不应影响副本
        original.recordHit();
        assertEquals(1, copy.getHitCount());
    }

    @Test
    void testReset() {
        CacheStats stats = new CacheStats();
        stats.recordHit();
        stats.recordMiss();
        stats.recordInsert();

        stats.reset();

        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getInsertCount());
        assertEquals(0, stats.getRequestCount());
        assertEquals(0.0, stats.getHitRate());
    }

    @Test
    void testMerge() {
        CacheStats stats1 = new CacheStats();
        stats1.recordHit();
        stats1.recordInsert();

        CacheStats stats2 = new CacheStats();
        stats2.recordHit();
        stats2.recordMiss();
        stats2.recordEviction();

        CacheStats merged = stats1.merge(stats2);

        assertEquals(2, merged.getHitCount());
        assertEquals(1, merged.getMissCount());
        assertEquals(1, merged.getInsertCount());
        assertEquals(1, merged.getEvictionCount());
    }

    @Test
    void testMergeAtomically() {
        CacheStats stats1 = new CacheStats();
        stats1.recordHit();

        CacheStats stats2 = new CacheStats();
        stats2.recordHit();
        stats2.recordMiss();

        stats1.mergeAtomically(stats2);

        assertEquals(2, stats1.getHitCount());
        assertEquals(1, stats1.getMissCount());
    }

    @Test
    void testToString() {
        CacheStats stats = new CacheStats();
        stats.recordHit();
        stats.recordMiss();

        String str = stats.toString();
        assertTrue(str.contains("hits=1"));
        assertTrue(str.contains("misses=1"));
        assertTrue(str.contains("hitRate"));
    }
}