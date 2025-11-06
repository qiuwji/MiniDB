package com.qiu.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
    private final long lockTimeoutMs;

    public ShardedCache(long capacity) {
        this(capacity, 16, 5000); // 默认16个分片，5秒超时
    }

    public ShardedCache(long capacity, int numShards) {
        this(capacity, numShards, 5000);
    }

    public ShardedCache(long capacity, int numShards, long lockTimeoutMs) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (numShards <= 0) {
            throw new IllegalArgumentException("Number of shards must be positive");
        }
        if (lockTimeoutMs <= 0) {
            throw new IllegalArgumentException("Lock timeout must be positive");
        }

        this.capacity = capacity;
        this.numShards = numShards;
        this.lockTimeoutMs = lockTimeoutMs;
        this.shards = new LRUCache[numShards];
        this.locks = new Lock[numShards];

        long baseShardCapacity = capacity / numShards;
        long remainder = capacity % numShards;

        for (int i = 0; i < numShards; i++) {
            // 前 remainder 个分片多分配1单位容量
            long shardCapacity = (i < remainder) ? baseShardCapacity + 1 : baseShardCapacity;
            shards[i] = new LRUCache(shardCapacity);
            locks[i] = new ReentrantLock();
        }
    }

    /**
     * 根据键计算分片索引（改进哈希分布）
     */
    int getShardIndex(String key) {
        int hash = key.hashCode();
        hash = hash ^ (hash >>> 16);
        return (hash & 0x7FFFFFFF) % numShards; // 确保非负
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
        try {
            if (lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    return getShard(key).put(key, value);
                } finally {
                    lock.unlock();
                }
            } else {
                throw new RuntimeException("Timeout acquiring lock for put operation, key: " + key);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring lock for put operation", e);
        }
    }

    @Override
    public CacheHandle get(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock lock = getShardLock(key);
        try {
            if (lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    return getShard(key).get(key);
                } finally {
                    lock.unlock();
                }
            } else {
                throw new RuntimeException("Timeout acquiring lock for get operation, key: " + key);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring lock for get operation", e);
        }
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "Key cannot be null");

        Lock lock = getShardLock(key);
        try {
            if (lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    return getShard(key).delete(key);
                } finally {
                    lock.unlock();
                }
            } else {
                throw new RuntimeException("Timeout acquiring lock for delete operation, key: " + key);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring lock for delete operation", e);
        }
    }

    @Override
    public void clear() {
        // 使用带超时的顺序锁定
        long startTime = System.currentTimeMillis();
        boolean[] locked = new boolean[numShards];

        try {
            for (int i = 0; i < numShards; i++) {
                if (!locks[i].tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timeout acquiring lock for shard " + i + " during clear");
                }
                locked[i] = true;

                // 检查总超时
                if (System.currentTimeMillis() - startTime > lockTimeoutMs) {
                    throw new RuntimeException("Clear operation timed out overall");
                }
            }

            // 执行清理
            for (LRUCache shard : shards) {
                shard.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring locks for clear", e);
        } finally {
            // 确保释放所有锁
            for (int i = 0; i < numShards; i++) {
                if (locked[i]) {
                    locks[i].unlock();
                }
            }
        }
    }

    @Override
    public long getSize() {
        long totalSize = 0;
        boolean[] locked = new boolean[numShards];
        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numShards; i++) {
                if (!locks[i].tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timeout acquiring lock for shard " + i + " during getSize");
                }
                locked[i] = true;

                if (System.currentTimeMillis() - startTime > lockTimeoutMs) {
                    throw new RuntimeException("getSize operation timed out overall");
                }
            }

            for (LRUCache shard : shards) {
                totalSize += shard.getSize();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring locks for getSize", e);
        } finally {
            for (int i = 0; i < numShards; i++) {
                if (locked[i]) {
                    locks[i].unlock();
                }
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
        boolean[] locked = new boolean[numShards];
        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numShards; i++) {
                if (!locks[i].tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timeout acquiring lock for shard " + i + " during getEntryCount");
                }
                locked[i] = true;

                if (System.currentTimeMillis() - startTime > lockTimeoutMs) {
                    throw new RuntimeException("getEntryCount operation timed out overall");
                }
            }

            for (LRUCache shard : shards) {
                totalCount += shard.getEntryCount();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring locks for getEntryCount", e);
        } finally {
            for (int i = 0; i < numShards; i++) {
                if (locked[i]) {
                    locks[i].unlock();
                }
            }
        }
        return totalCount;
    }

    @Override
    public void releaseHandle(CacheHandle handle) {
        String key = handle.getKey();
        Lock lock = getShardLock(key);
        try {
            if (lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    getShard(key).releaseHandle(handle);
                } finally {
                    lock.unlock();
                }
            } else {
                throw new RuntimeException("Timeout acquiring lock for releaseHandle operation");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring lock for releaseHandle", e);
        }
    }

    @Override
    public CacheStats getStats() {
        CacheStats totalStats = new CacheStats();
        boolean[] locked = new boolean[numShards];
        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numShards; i++) {
                if (!locks[i].tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timeout acquiring lock for shard " + i + " during getStats");
                }
                locked[i] = true;

                if (System.currentTimeMillis() - startTime > lockTimeoutMs) {
                    throw new RuntimeException("getStats operation timed out overall");
                }
            }

            for (LRUCache shard : shards) {
                CacheStats shardStats = shard.getStats();
                totalStats.mergeAtomically(shardStats);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring locks for getStats", e);
        } finally {
            for (int i = 0; i < numShards; i++) {
                if (locked[i]) {
                    locks[i].unlock();
                }
            }
        }
        return totalStats;
    }

    @Override
    public void pruneTo(long targetSize) {
        if (targetSize < 0) {
            throw new IllegalArgumentException("Target size cannot be negative");
        }

        // 更精确的分片目标容量计算
        long baseShardTarget = targetSize / numShards;
        long remainder = targetSize % numShards;

        boolean[] locked = new boolean[numShards];
        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numShards; i++) {
                if (!locks[i].tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timeout acquiring lock for shard " + i + " during pruneTo");
                }
                locked[i] = true;

                if (System.currentTimeMillis() - startTime > lockTimeoutMs) {
                    throw new RuntimeException("pruneTo operation timed out overall");
                }
            }

            for (int i = 0; i < numShards; i++) {
                long shardTarget = (i < remainder) ? baseShardTarget + 1 : baseShardTarget;
                shards[i].pruneTo(shardTarget);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring locks for pruneTo", e);
        } finally {
            for (int i = 0; i < numShards; i++) {
                if (locked[i]) {
                    locks[i].unlock();
                }
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

        Lock lock = locks[shardIndex];
        try {
            if (lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    return shards[shardIndex].getStats();
                } finally {
                    lock.unlock();
                }
            } else {
                throw new RuntimeException("Timeout acquiring lock for shard " + shardIndex);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring lock for shard " + shardIndex, e);
        }
    }

    /**
     * 获取分片负载信息（用于监控）
     */
    public double[] getShardLoadFactors() {
        double[] loadFactors = new double[numShards];
        boolean[] locked = new boolean[numShards];
        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numShards; i++) {
                if (!locks[i].tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timeout acquiring lock for shard " + i + " during getShardLoadFactors");
                }
                locked[i] = true;

                if (System.currentTimeMillis() - startTime > lockTimeoutMs) {
                    throw new RuntimeException("getShardLoadFactors operation timed out overall");
                }
            }

            for (int i = 0; i < numShards; i++) {
                loadFactors[i] = shards[i].getLoadFactor();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring locks for getShardLoadFactors", e);
        } finally {
            for (int i = 0; i < numShards; i++) {
                if (locked[i]) {
                    locks[i].unlock();
                }
            }
        }
        return loadFactors;
    }

    @Override
    public String toString() {
        CacheStats stats = getStats();
        return String.format("ShardedCache{shards=%d, size=%d, capacity=%d, entries=%d, hitRate=%.3f}",
                numShards, getSize(), capacity, getEntryCount(), stats.getHitRate());
    }
}