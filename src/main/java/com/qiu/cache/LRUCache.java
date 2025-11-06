package com.qiu.cache;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于LRU算法的缓存实现
 */
public class LRUCache implements Cache {
    private final long capacity;
    private long currentSize;
    private final LinkedHashMap<String, CacheEntry> cache; // 唯一存储，同时负责LRU顺序
    private final ReadWriteLock lock;
    private final CacheStats stats;

    public LRUCache(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }

        this.capacity = capacity;
        this.currentSize = 0;
        this.cache = new LinkedHashMap<String, CacheEntry>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
                // 容量检查在put时统一处理，这里不自动移除
                return false;
            }
        };
        this.lock = new ReentrantReadWriteLock();
        this.stats = new CacheStats();
    }

    @Override
    public CacheHandle put(String key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            // 如果键已存在，先删除旧条目
            if (cache.containsKey(key)) {
                deleteInternal(key);
            }

            // 创建新条目
            CacheEntry entry = new CacheEntry(key, value);
            long entrySize = entry.getSize();

            // 检查容量，必要时淘汰旧条目
            while (currentSize + entrySize > capacity && !cache.isEmpty()) {
                evictOldest();
            }

            // 最终容量检查
            if (currentSize + entrySize > capacity) {
                stats.recordMiss();
                return null;
            }

            // 插入新条目
            cache.put(key, entry);
            currentSize += entrySize;

            stats.recordInsert();
            return new CacheHandle(entry, this);

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public CacheHandle get(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            CacheEntry entry = cache.get(key); // LinkedHashMap会自动调整访问顺序
            if (entry != null) {
                entry.recordAccess();
                stats.recordHit();
                return new CacheHandle(entry, this);
            } else {
                stats.recordMiss();
                return null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            return deleteInternal(key);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 内部删除方法（假设已经持有写锁）
     */
    private boolean deleteInternal(String key) {
        CacheEntry entry = cache.remove(key);
        if (entry != null) {
            currentSize -= entry.getSize();
            stats.recordDelete();
            return true;
        }
        return false;
    }

    @Override
    public void clear() {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            cache.clear();
            currentSize = 0;
            stats.reset();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getSize() {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            return currentSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public int getEntryCount() {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            return cache.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void releaseHandle(CacheHandle handle) {
        // LRU缓存中，句柄释放不需要特殊操作
        // 访问计数已经在创建句柄时更新
    }

    @Override
    public CacheStats getStats() {
        return new CacheStats(stats); // 返回拷贝
    }

    @Override
    public void pruneTo(long targetSize) {
        if (targetSize < 0) {
            throw new IllegalArgumentException("Target size cannot be negative");
        }
        if (targetSize >= capacity) {
            return; // 不需要修剪
        }

        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            while (currentSize > targetSize && !cache.isEmpty()) {
                evictOldest();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 淘汰最旧的条目
     */
    private void evictOldest() {
        if (cache.isEmpty()) {
            return;
        }

        // LinkedHashMap的iterator按插入顺序遍历，第一个是最旧的
        Iterator<Map.Entry<String, CacheEntry>> it = cache.entrySet().iterator();
        if (it.hasNext()) {
            Map.Entry<String, CacheEntry> oldest = it.next();
            String key = oldest.getKey();
            CacheEntry entry = oldest.getValue();

            // 从缓存中删除
            it.remove();
            currentSize -= entry.getSize();

            stats.recordEviction();
        }
    }

    /**
     * 获取所有键的列表（用于测试和监控）
     */
    public List<String> getKeys() {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            return new ArrayList<>(cache.keySet());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取缓存使用率（0.0 - 1.0）
     */
    public double getUsageRatio() {
        return (double) currentSize / capacity;
    }

    /**
     * 获取淘汰次数（用于监控）
     */
    public long getEvictionCount() {
        return stats.getEvictionCount();
    }

    /**
     * 获取负载因子（当前大小/容量）
     */
    public double getLoadFactor() {
        return (double) currentSize / capacity;
    }

    /**
     * 获取内存使用详情
     */
    public MemoryUsage getMemoryUsage() {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            long entryOverhead = cache.size() * 100; // 估算每个条目的Map开销
            long dataSize = currentSize;
            return new MemoryUsage(dataSize, entryOverhead, capacity);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String toString() {
        return String.format("LRUCache{size=%d, capacity=%d, entries=%d, hitRate=%.2f, load=%.2f}",
                currentSize, capacity, getEntryCount(), stats.getHitRate(), getLoadFactor());
    }

    /**
     * 内存使用信息
     */
    public static class MemoryUsage {
        private final long dataSize;
        private final long overheadSize;
        private final long capacity;

        public MemoryUsage(long dataSize, long overheadSize, long capacity) {
            this.dataSize = dataSize;
            this.overheadSize = overheadSize;
            this.capacity = capacity;
        }

        public long getDataSize() { return dataSize; }
        public long getOverheadSize() { return overheadSize; }
        public long getCapacity() { return capacity; }
        public long getTotalSize() { return dataSize + overheadSize; }

        public double getUsageRatio() {
            return (double) getTotalSize() / capacity;
        }

        @Override
        public String toString() {
            return String.format("MemoryUsage{data=%.2fKB, overhead=%.2fKB, total=%.2fKB, capacity=%.2fKB, usage=%.1f%%}",
                    dataSize / 1024.0, overheadSize / 1024.0, getTotalSize() / 1024.0,
                    capacity / 1024.0, getUsageRatio() * 100);
        }
    }
}