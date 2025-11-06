package com.qiu.cache;

import com.qiu.sstable.Block;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 专门用于缓存SSTable块的缓存实现
 */
public class BlockCache {
    private final Cache cache;
    private final String cachePrefix;

    // 维护表到块键的映射关系
    private final Map<String, Set<String>> tableToBlocksMap;
    private final ReadWriteLock mappingLock;

    // 统计信息
    private final CacheStats invalidationStats;

    public BlockCache(long capacity) {
        this(capacity, 16);
    }

    public BlockCache(long capacity, int shards) {
        this.cache = new ShardedCache(capacity, shards);
        this.cachePrefix = "block:";
        this.tableToBlocksMap = new ConcurrentHashMap<>();
        this.mappingLock = new ReentrantReadWriteLock();
        this.invalidationStats = new CacheStats();
    }

    /**
     * 插入块到缓存，新增version字段，避免缓存与DB的不一致
     * === 修改点: 接受 byte[] blockData 而不是 Block block ===
     */
    public void put(String tableName, long blockOffset, byte[] blockData, long versionId) {
        String key = buildKey(tableName, blockOffset, versionId);
        // byte[] blockData = serializeBlock(block); // <-- 移除序列化
        if (blockData != null) {
            CacheHandle handle = cache.put(key, blockData);
            if (handle != null) {
                // 成功插入，更新映射关系
                addBlockMapping(tableName, key);
            }
        }
    }

    /**
     * 从缓存查找块
     * === 修改点: 返回 byte[] 而不是 Block ===
     */
    public byte[] get(String tableName, long blockOffset, long versionId) {
        String key = buildKey(tableName, blockOffset, versionId);
        try (CacheHandle handle = cache.get(key)) {
            if (handle != null) {
                // byte[] blockData = handle.getData(); // <-- 移除反序列化
                // return deserializeBlock(blockData);
                return handle.getData(); // <-- 直接返回原始 byte[]
            }
        } catch (Exception e) {
            System.err.println("Error deserializing block from cache: " + e.getMessage());
        }
        return null;
    }

    /**
     * 删除表的所有块
     */
    public void invalidateTable(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        int invalidatedCount = 0;
        long freedSize = 0;

        Lock writeLock = mappingLock.writeLock();
        writeLock.lock();
        try {
            Set<String> blockKeys = tableToBlocksMap.remove(tableName);
            if (blockKeys != null && !blockKeys.isEmpty()) {
                // 批量删除缓存条目
                for (String blockKey : blockKeys) {
                    // 先获取块大小（用于统计）
                    try (CacheHandle handle = cache.get(blockKey)) {
                        if (handle != null) {
                            freedSize += handle.getSize();
                        }
                    } catch (Exception e) {
                        // 忽略获取大小时的异常
                    }

                    // 删除缓存条目
                    if (cache.delete(blockKey)) {
                        invalidatedCount++;
                    }
                }

                // 记录统计信息
                recordInvalidationStats(invalidatedCount, freedSize);

                System.out.printf("Invalidated table '%s': %d blocks, %.2f KB freed%n",
                        tableName, invalidatedCount, freedSize / 1024.0);
            } else {
                System.out.printf("No blocks found for table '%s' in cache%n", tableName);
            }
        } finally {
            writeLock.unlock();
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Table invalidation completed in %d ms%n", duration);
    }

    /**
     * 批量失效多个表
     */
    public void invalidateTables(Collection<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        int totalInvalidated = 0;
        long totalFreedSize = 0;

        Lock writeLock = mappingLock.writeLock();
        writeLock.lock();
        try {
            for (String tableName : tableNames) {
                Set<String> blockKeys = tableToBlocksMap.remove(tableName);
                if (blockKeys != null) {
                    int tableInvalidated = 0;
                    long tableFreedSize = 0;

                    for (String blockKey : blockKeys) {
                        // 获取块大小
                        try (CacheHandle handle = cache.get(blockKey)) {
                            if (handle != null) {
                                tableFreedSize += handle.getSize();
                            }
                        } catch (Exception e) {
                            // 忽略异常
                        }

                        if (cache.delete(blockKey)) {
                            tableInvalidated++;
                        }
                    }

                    totalInvalidated += tableInvalidated;
                    totalFreedSize += tableFreedSize;

                    System.out.printf("  - Table '%s': %d blocks, %.2f KB%n",
                            tableName, tableInvalidated, tableFreedSize / 1024.0);
                }
            }

            recordInvalidationStats(totalInvalidated, totalFreedSize);
        } finally {
            writeLock.unlock();
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Batch invalidation completed: %d tables, %d blocks, %.2f KB freed, %d ms%n",
                tableNames.size(), totalInvalidated, totalFreedSize / 1024.0, duration);
    }

    /**
     * 基于模式匹配失效表（支持通配符）
     */
    public void invalidateTablePattern(String pattern) {
        if (pattern == null || pattern.trim().isEmpty()) {
            return;
        }

        List<String> matchedTables = new ArrayList<>();
        Lock readLock = mappingLock.readLock();
        readLock.lock();
        try {
            for (String tableName : tableToBlocksMap.keySet()) {
                if (matchesPattern(tableName, pattern)) {
                    matchedTables.add(tableName);
                }
            }
        } finally {
            readLock.unlock();
        }

        if (!matchedTables.isEmpty()) {
            invalidateTables(matchedTables);
        } else {
            System.out.printf("No tables matching pattern '%s' found in cache%n", pattern);
        }
    }

    /**
     * 获取表的缓存统计信息
     */
    public TableCacheStats getTableStats(String tableName) {
        if (tableName == null) {
            return new TableCacheStats("", 0, 0, 0);
        }

        Lock readLock = mappingLock.readLock();
        readLock.lock();
        try {
            Set<String> blockKeys = tableToBlocksMap.get(tableName);
            if (blockKeys == null || blockKeys.isEmpty()) {
                return new TableCacheStats(tableName, 0, 0, 0);
            }

            int blockCount = blockKeys.size();
            long totalSize = 0;
            int accessibleBlocks = 0;

            for (String blockKey : blockKeys) {
                try (CacheHandle handle = cache.get(blockKey)) {
                    if (handle != null) {
                        totalSize += handle.getSize();
                        accessibleBlocks++;
                    }
                } catch (Exception e) {
                    // 忽略异常
                }
            }

            return new TableCacheStats(tableName, blockCount, accessibleBlocks, totalSize);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取所有缓存的表名
     */
    public Set<String> getCachedTables() {
        Lock readLock = mappingLock.readLock();
        readLock.lock();
        try {
            return new HashSet<>(tableToBlocksMap.keySet());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 清理无效的映射关系（维护性操作）
     */
    public void cleanupStaleMappings() {
        long startTime = System.currentTimeMillis();
        int removedMappings = 0;
        int checkedTables = 0;

        Lock writeLock = mappingLock.writeLock();
        writeLock.lock();
        try {
            Iterator<Map.Entry<String, Set<String>>> iterator = tableToBlocksMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Set<String>> entry = iterator.next();
                String tableName = entry.getKey();
                Set<String> blockKeys = entry.getValue();

                checkedTables++;

                // 清理无效的块键
                Iterator<String> keyIterator = blockKeys.iterator();
                while (keyIterator.hasNext()) {
                    String blockKey = keyIterator.next();
                    try (CacheHandle handle = cache.get(blockKey)) {
                        if (handle == null) {
                            // 缓存中不存在，移除映射
                            keyIterator.remove();
                            removedMappings++;
                        }
                    } catch (Exception e) {
                        // 获取失败，也移除映射
                        keyIterator.remove();
                        removedMappings++;
                    }
                }

                // 如果表没有有效的块了，移除整个表条目
                if (blockKeys.isEmpty()) {
                    iterator.remove();
                }
            }
        } finally {
            writeLock.unlock();
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Cleanup completed: checked %d tables, removed %d stale mappings, %d ms%n",
                checkedTables, removedMappings, duration);
    }

    /**
     * 添加块映射关系
     */
    private void addBlockMapping(String tableName, String blockKey) {
        Lock writeLock = mappingLock.writeLock();
        writeLock.lock();
        try {
            tableToBlocksMap.computeIfAbsent(tableName, k -> ConcurrentHashMap.newKeySet())
                    .add(blockKey);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 构建缓存键
     */
    private String buildKey(String tableName, long blockOffset, long versionId) {
        return cachePrefix + String.format("%s:%d:%d", tableName, blockOffset, versionId);
    }

    /**
     * 从缓存键解析表名
     */
    private String parseTableNameFromKey(String blockKey) {
        if (!blockKey.startsWith(cachePrefix)) {
            return null;
        }
        String withoutPrefix = blockKey.substring(cachePrefix.length());
        int colonIndex = withoutPrefix.indexOf(':');
        if (colonIndex == -1) {
            return null;
        }
        return withoutPrefix.substring(0, colonIndex);
    }

    /**
     * 模式匹配（支持 * 和 ? 通配符）
     */
    private boolean matchesPattern(String text, String pattern) {
        if ("*".equals(pattern)) {
            return true;
        }

        // 简单的通配符匹配实现
        String regex = pattern.replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return text.matches(regex);
    }

    /**
     * 记录失效统计信息
     */
    private void recordInvalidationStats(int count, long size) {
        for (int i = 0; i < count; i++) {
            invalidationStats.recordDelete();
        }
        // 这里可以记录更多详细的统计信息
    }

    // 原有的公共方法
    public CacheStats getStats() {
        return cache.getStats();
    }

    public CacheStats getInvalidationStats() {
        return new CacheStats(invalidationStats);
    }

    public long getSize() {
        return cache.getSize();
    }

    public long getCapacity() {
        return cache.getCapacity();
    }

    public void clear() {
        Lock writeLock = mappingLock.writeLock();
        writeLock.lock();
        try {
            cache.clear();
            tableToBlocksMap.clear();
            invalidationStats.reset();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        int tableCount = getCachedTables().size();
        return String.format("BlockCache{tables=%d, %s}", tableCount, cache.toString());
    }

    /**
     * 表缓存统计信息
     */
    public static class TableCacheStats {
        private final String tableName;
        private final int totalBlocks;
        private final int accessibleBlocks;
        private final long totalSize;

        public TableCacheStats(String tableName, int totalBlocks, int accessibleBlocks, long totalSize) {
            this.tableName = tableName;
            this.totalBlocks = totalBlocks;
            this.accessibleBlocks = accessibleBlocks;
            this.totalSize = totalSize;
        }

        public String getTableName() { return tableName; }
        public int getTotalBlocks() { return totalBlocks; }
        public int getAccessibleBlocks() { return accessibleBlocks; }
        public long getTotalSize() { return totalSize; }
        public double getAccessibilityRatio() {
            return totalBlocks > 0 ? (double) accessibleBlocks / totalBlocks : 0.0;
        }

        @Override
        public String toString() {
            return String.format("TableCacheStats{table='%s', blocks=%d/%d, size=%.2fKB, accessibility=%.1f%%}",
                    tableName, accessibleBlocks, totalBlocks, totalSize / 1024.0,
                    getAccessibilityRatio() * 100);
        }
    }
}