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
    private final Map<String, CacheEntry> entries;
    private final LinkedHashMap<String, CacheEntry> accessOrder;
    private final ReadWriteLock lock;
    private final CacheStats stats;

    public LRUCache(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }

        this.capacity = capacity;
        this.currentSize = 0;
        this.entries = new HashMap<>();
        this.accessOrder = new LinkedHashMap<>(16, 0.75f, true); // 访问顺序
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
            if (entries.containsKey(key)) {
                deleteInternal(key);
            }

            // 创建新条目
            CacheEntry entry = new CacheEntry(key, value);
            long entrySize = entry.getSize();

            // 检查容量，必要时淘汰旧条目
            while (currentSize + entrySize > capacity && !entries.isEmpty()) {
                evictOldest();
            }

            // 如果仍然没有足够空间，拒绝插入
            if (currentSize + entrySize > capacity) {
                stats.recordMiss();
                return null;
            }

            // 插入新条目
            entries.put(key, entry);
            accessOrder.put(key, entry);
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

        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            CacheEntry entry = entries.get(key);
            if (entry != null) {
                // 更新访问顺序
                accessOrder.get(key); // 调用get方法会更新LinkedHashMap的访问顺序
                entry.recordAccess();
                stats.recordHit();
                return new CacheHandle(entry, this);
            } else {
                stats.recordMiss();
                return null;
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock writeLock = lock.writeLock();
        writeLock.unlock();
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
        CacheEntry entry = entries.remove(key);
        if (entry != null) {
            accessOrder.remove(key);
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
            entries.clear();
            accessOrder.clear();
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
            return entries.size();
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
            while (currentSize > targetSize && !entries.isEmpty()) {
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
        if (accessOrder.isEmpty()) {
            return;
        }

        // LinkedHashMap的iterator按插入顺序遍历，第一个是最旧的
        Iterator<Map.Entry<String, CacheEntry>> it = accessOrder.entrySet().iterator();
        if (it.hasNext()) {
            Map.Entry<String, CacheEntry> oldest = it.next();
            String key = oldest.getKey();
            CacheEntry entry = oldest.getValue();

            // 从两个映射中删除
            entries.remove(key);
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
            return new ArrayList<>(entries.keySet());
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

    @Override
    public String toString() {
        return String.format("LRUCache{size=%d, capacity=%d, entries=%d, hitRate=%.2f}",
                currentSize, capacity, getEntryCount(), stats.getHitRate());
    }
}
