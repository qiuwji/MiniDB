package com.qiu.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 缓存统计信息
 */
public class CacheStats {
    private final AtomicLong hitCount;
    private final AtomicLong missCount;
    private final AtomicLong insertCount;
    private final AtomicLong deleteCount;
    private final AtomicLong evictionCount;

    public CacheStats() {
        this.hitCount = new AtomicLong(0);
        this.missCount = new AtomicLong(0);
        this.insertCount = new AtomicLong(0);
        this.deleteCount = new AtomicLong(0);
        this.evictionCount = new AtomicLong(0);
    }

    public CacheStats(CacheStats other) {
        this.hitCount = new AtomicLong(other.hitCount.get());
        this.missCount = new AtomicLong(other.missCount.get());
        this.insertCount = new AtomicLong(other.insertCount.get());
        this.deleteCount = new AtomicLong(other.deleteCount.get());
        this.evictionCount = new AtomicLong(other.evictionCount.get());
    }

    public void recordHit() {
        hitCount.incrementAndGet();
    }

    public void recordMiss() {
        missCount.incrementAndGet();
    }

    public void recordInsert() {
        insertCount.incrementAndGet();
    }

    public void recordDelete() {
        deleteCount.incrementAndGet();
    }

    public void recordEviction() {
        evictionCount.incrementAndGet();
    }

    public long getHitCount() {
        return hitCount.get();
    }

    public long getMissCount() {
        return missCount.get();
    }

    public long getInsertCount() {
        return insertCount.get();
    }

    public long getDeleteCount() {
        return deleteCount.get();
    }

    public long getEvictionCount() {
        return evictionCount.get();
    }

    public long getRequestCount() {
        return hitCount.get() + missCount.get();
    }

    public double getHitRate() {
        long requests = getRequestCount();
        return requests > 0 ? (double) hitCount.get() / requests : 0.0;
    }

    public double getMissRate() {
        long requests = getRequestCount();
        return requests > 0 ? (double) missCount.get() / requests : 0.0;
    }

    public void reset() {
        hitCount.set(0);
        missCount.set(0);
        insertCount.set(0);
        deleteCount.set(0);
        evictionCount.set(0);
    }

    /**
     * 合并两个统计对象
     */
    public CacheStats merge(CacheStats other) {
        CacheStats merged = new CacheStats();
        merged.hitCount.set(this.hitCount.get() + other.hitCount.get());
        merged.missCount.set(this.missCount.get() + other.missCount.get());
        merged.insertCount.set(this.insertCount.get() + other.insertCount.get());
        merged.deleteCount.set(this.deleteCount.get() + other.deleteCount.get());
        merged.evictionCount.set(this.evictionCount.get() + other.evictionCount.get());
        return merged;
    }

    @Override
    public String toString() {
        return String.format("CacheStats{hits=%d, misses=%d, hitRate=%.3f, inserts=%d, deletes=%d, evictions=%d}",
                hitCount.get(), missCount.get(), getHitRate(),
                insertCount.get(), deleteCount.get(), evictionCount.get());
    }
}