package com.qiu.cache;

import com.qiu.sstable.Block;

/**
 * 专门用于缓存SSTable块的缓存实现
 */
public class BlockCache {
    private final Cache cache;
    private final String cachePrefix;

    public BlockCache(long capacity) {
        this.cache = new ShardedCache(capacity);
        this.cachePrefix = "block:";
    }

    public BlockCache(long capacity, int shards) {
        this.cache = new ShardedCache(capacity, shards);
        this.cachePrefix = "block:";
    }

    /**
     * 插入块到缓存
     */
    public void put(String tableName, long blockOffset, Block block) {
        String key = buildKey(tableName, blockOffset);
        byte[] blockData = serializeBlock(block);
        cache.put(key, blockData);
    }

    /**
     * 从缓存查找块
     */
    public Block get(String tableName, long blockOffset) {
        String key = buildKey(tableName, blockOffset);
        try (CacheHandle handle = cache.get(key)) {
            if (handle != null) {
                byte[] blockData = handle.getData();
                return deserializeBlock(blockData);
            }
        }
        return null;
    }

    /**
     * 删除表的所有块（当表被删除时）
     */
    public void invalidateTable(String tableName) {
        // 简化实现：在实际系统中需要更高效的方式
        // 这里只是演示概念
        String prefix = cachePrefix + tableName + ":";
        // 注意：实际实现需要维护表到块的映射关系
    }

    /**
     * 构建缓存键
     */
    private String buildKey(String tableName, long blockOffset) {
        return cachePrefix + tableName + ":" + blockOffset;
    }

    /**
     * 序列化块（简化实现）
     */
    private byte[] serializeBlock(Block block) {
        // 简化实现：实际需要根据Block的具体结构进行序列化
        // 这里返回空数组作为示例
        return new byte[0];
    }

    /**
     * 反序列化块（简化实现）
     */
    private Block deserializeBlock(byte[] data) {
        // 简化实现：实际需要根据数据重建Block对象
        // 这里返回null作为示例
        return null;
    }

    public CacheStats getStats() {
        return cache.getStats();
    }

    public long getSize() {
        return cache.getSize();
    }

    public long getCapacity() {
        return cache.getCapacity();
    }

    public void clear() {
        cache.clear();
    }

    @Override
    public String toString() {
        return String.format("BlockCache{%s}", cache.toString());
    }
}
