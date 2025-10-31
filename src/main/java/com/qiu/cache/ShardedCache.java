package com.qiu.cache;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分片缓存，将缓存分成多个分片以减少锁竞争
 */
public class ShardedCache implements Cache {
    private final int numShards;
    private final LRUCache[] shards;
    private final Lock[] locks;
    private final long capacity;

    public ShardedCache(long capacity) {
        this(capacity, 16); // 默认16个分片
    }

    public ShardedCache(long capacity, int numShards) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (numShards <= 0) {
            throw new IllegalArgumentException("Number of shards must be positive");
        }

        this.capacity = capacity;
        this.numShards = numShards;
        this.shards = new LRUCache[numShards];
        this.locks = new Lock[numShards];

        long shardCapacity = capacity / numShards;
        for (int i = 0; i < numShards; i++) {
            shards[i] = new LRUCache(shardCapacity);
            locks[i] = new ReentrantLock();
        }
    }

    /**
     * 根据键计算分片索引
     */
    private int getShardIndex(String key) {
        return Math.abs(key.hashCode()) % numShards;
    }

    /**
     * 获取指定分片的缓存
     */
    private LRUCache getShard(String key) {
        return shards[getShardIndex(key)];
    }

    /**
     * 获取指定分片的锁
     */
    private Lock getShardLock(String key) {
        return locks[getShardIndex(key)];
    }

    @Override
    public CacheHandle put(String key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        Lock lock = getShardLock(key);
        lock.lock();
        try {
            return getShard(key).put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CacheHandle get(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock lock = getShardLock(key);
        lock.lock();
        try {
            return getShard(key).get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock lock = getShardLock(key);
        lock.lock();
        try {
            return getShard(key).delete(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        // 按顺序锁定所有分片以避免死锁
        for (int i = 0; i < numShards; i++) {
            locks[i].lock();
        }
        try {
            for (LRUCache shard : shards) {
                shard.clear();
            }
        } finally {
            for (int i = 0; i < numShards; i++) {
                locks[i].unlock();
            }
        }
    }

    @Override
    public long getSize() {
        long totalSize = 0;
        for (int i = 0; i < numShards; i++) {
            locks[i].lock();
        }
        try {
            for (LRUCache shard : shards) {
                totalSize += shard.getSize();
            }
        } finally {
            for (int i = 0; i < numShards; i++) {
                locks[i].unlock();
            }
        }
        return totalSize;
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public int getEntryCount() {
        int totalCount = 0;
        for (int i = 0; i < numShards; i++) {
            locks[i].lock();
        }
        try {
            for (LRUCache shard : shards) {
                totalCount += shard.getEntryCount();
            }
        } finally {
            for (int i = 0; i < numShards; i++) {
                locks[i].unlock();
            }
        }
        return totalCount;
    }

    @Override
    public void releaseHandle(CacheHandle handle) {
        // 分片缓存中，句柄释放委托给对应的分片
        String key = handle.getKey();
        Lock lock = getShardLock(key);
        lock.lock();
        try {
            getShard(key).releaseHandle(handle);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CacheStats getStats() {
        CacheStats totalStats = new CacheStats();
        for (int i = 0; i < numShards; i++) {
            locks[i].lock();
        }
        try {
            for (LRUCache shard : shards) {
                CacheStats shardStats = shard.getStats();
                totalStats = totalStats.merge(shardStats);
            }
        } finally {
            for (int i = 0; i < numShards; i++) {
                locks[i].unlock();
            }
        }
        return totalStats;
    }

    @Override
    public void pruneTo(long targetSize) {
        if (targetSize < 0) {
            throw new IllegalArgumentException("Target size cannot be negative");
        }

        long shardTarget = targetSize / numShards;
        for (int i = 0; i < numShards; i++) {
            locks[i].lock();
        }
        try {
            for (LRUCache shard : shards) {
                shard.pruneTo(shardTarget);
            }
        } finally {
            for (int i = 0; i < numShards; i++) {
                locks[i].unlock();
            }
        }
    }

    /**
     * 获取分片数量
     */
    public int getShardCount() {
        return numShards;
    }

    /**
     * 获取指定分片的统计信息
     */
    public CacheStats getShardStats(int shardIndex) {
        if (shardIndex < 0 || shardIndex >= numShards) {
            throw new IllegalArgumentException("Invalid shard index");
        }

        locks[shardIndex].lock();
        try {
            return shards[shardIndex].getStats();
        } finally {
            locks[shardIndex].unlock();
        }
    }

    @Override
    public String toString() {
        return String.format("ShardedCache{shards=%d, totalSize=%d, capacity=%d, hitRate=%.2f}",
                numShards, getSize(), capacity, getStats().getHitRate());
    }
}
