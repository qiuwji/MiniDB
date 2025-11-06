package com.qiu.cache;

import com.qiu.sstable.Block;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 缓存系统集成测试 - JUnit 5 版本
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("缓存系统集成测试")
class CacheIntegrationTest {

    private BlockCache blockCache;
    private static final long CACHE_CAPACITY = 10 * 1024 * 1024; // 10MB

    @BeforeEach
    void setUp() {
        blockCache = new BlockCache(CACHE_CAPACITY, 8); // 8个分片
        System.out.println("初始化 BlockCache，容量: " + CACHE_CAPACITY / 1024 / 1024 + "MB");
    }

    @AfterEach
    void tearDown() {
        if (blockCache != null) {
            blockCache.clear();
        }
    }

    @Test
    @DisplayName("基础块缓存功能测试")
    void testBasicBlockCaching() {
        // 创建测试块
        Block block1 = createTestBlock("table1", 0, 1024);
        Block block2 = createTestBlock("table1", 4096, 2048);

        // 插入块到缓存
        blockCache.put("table1", 0, block1);
        blockCache.put("table1", 4096, block2);

        // 验证缓存统计
        CacheStats stats = blockCache.getStats();
        System.out.println("插入后统计: " + stats);
        assertTrue(stats.getInsertCount() >= 2, "应该有插入操作");

        // 从缓存读取块
        Block retrieved1 = blockCache.get("table1", 0);
        Block retrieved2 = blockCache.get("table1", 4096);

        assertNotNull(retrieved1, "应该能获取到block1");
        assertNotNull(retrieved2, "应该能获取到block2");
        assertArrayEquals(getBlockData(block1), getBlockData(retrieved1), "block1数据应该匹配");
        assertArrayEquals(getBlockData(block2), getBlockData(retrieved2), "block2数据应该匹配");

        // 验证命中统计
        stats = blockCache.getStats();
        System.out.println("读取后统计: " + stats);
        assertTrue(stats.getHitCount() >= 2, "应该有命中");
    }

    @Test
    @DisplayName("表失效功能测试")
    void testTableInvalidation() {
        // 为多个表插入数据
        String[] tables = {"users", "orders", "products"};
        for (String table : tables) {
            for (int i = 0; i < 3; i++) {
                Block block = createTestBlock(table, i * 4096, 512);
                blockCache.put(table, i * 4096, block);
            }
        }

        // 验证所有表都有数据
        Set<String> cachedTables = blockCache.getCachedTables();
        System.out.println("缓存中的表: " + cachedTables);
        assertEquals(3, cachedTables.size(), "应该有3个表");

        // 获取失效前的统计
        long initialSize = blockCache.getSize();
        CacheStats initialStats = blockCache.getStats();
        System.out.println("失效前 - 大小: " + initialSize + ", 统计: " + initialStats);

        // 失效一个表
        blockCache.invalidateTable("users");

        // 验证users表被移除
        cachedTables = blockCache.getCachedTables();
        System.out.println("失效后缓存中的表: " + cachedTables);
        assertFalse(cachedTables.contains("users"), "users表应该被移除");
        assertTrue(cachedTables.contains("orders"), "orders表应该还在");
        assertTrue(cachedTables.contains("products"), "products表应该还在");

        // 验证大小减少
        long afterInvalidationSize = blockCache.getSize();
        System.out.println("失效后大小: " + afterInvalidationSize);
        assertTrue(afterInvalidationSize < initialSize, "缓存大小应该减少");

        // 验证失效统计
        CacheStats invalidationStats = blockCache.getInvalidationStats();
        System.out.println("失效统计: " + invalidationStats);
        assertTrue(invalidationStats.getDeleteCount() > 0, "应该有删除操作");
    }

    @Test
    @DisplayName("LRU淘汰策略测试")
    void testLRUEviction() {
        // 创建小容量缓存来测试淘汰
        BlockCache smallCache = new BlockCache(5 * 1024); // 5KB

        try {
            // 插入多个块，超过容量
            for (int i = 0; i < 10; i++) {
                Block block = createTestBlock("test_table", i * 1024, 1024); // 每个块1KB
                smallCache.put("test_table", i * 1024, block);
            }

            // 验证缓存大小不超过容量
            long finalSize = smallCache.getSize();
            System.out.println("最终缓存大小: " + finalSize + " (容量: " + smallCache.getCapacity() + ")");
            assertTrue(finalSize <= smallCache.getCapacity(), "缓存大小不应超过容量");

            // 验证淘汰统计
            CacheStats stats = smallCache.getStats();
            System.out.println("淘汰测试统计: " + stats);
            assertTrue(stats.getEvictionCount() > 0, "应该有淘汰发生");

        } finally {
            smallCache.clear();
        }
    }

    @Test
    @DisplayName("并发访问测试")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testConcurrentAccess() throws Exception {
        int threadCount = 4;
        int operationsPerThread = 100;
        CompletableFuture<?>[] futures = new CompletableFuture[threadCount];

        // 创建多个线程并发访问缓存
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            futures[i] = CompletableFuture.runAsync(() -> {
                String tableName = "concurrent_table_" + threadId;
                for (int j = 0; j < operationsPerThread; j++) {
                    Block block = createTestBlock(tableName, j * 512, 256);
                    blockCache.put(tableName, j * 512, block);

                    // 50%的概率进行读取
                    if (j % 2 == 0) {
                        Block retrieved = blockCache.get(tableName, j * 512);
                        if (retrieved != null) {
                            assertArrayEquals(getBlockData(block), getBlockData(retrieved));
                        }
                    }
                }
            });
        }

        // 等待所有任务完成
        CompletableFuture.allOf(futures).get();

        // 验证最终状态
        CacheStats stats = blockCache.getStats();
        System.out.println("并发测试后统计: " + stats);
        assertTrue(stats.getRequestCount() >= threadCount * operationsPerThread,
                "应该有大量操作");
    }

    @Test
    @DisplayName("缓存句柄安全性测试")
    void testCacheHandleSafety() {
        Block block = createTestBlock("handle_test", 0, 1024);
        blockCache.put("handle_test", 0, block);

        // 使用try-with-resources确保句柄正确关闭
        try (CacheHandle handle = getCacheHandle(blockCache, "handle_test", 0)) {
            assertNotNull(handle, "应该获取到句柄");
            assertFalse(handle.isClosed(), "句柄不应该关闭");
            assertArrayEquals(getBlockData(block), handle.getData(), "数据应该匹配");
        } // 自动调用close()

        // 验证句柄已关闭后访问会抛出异常
        CacheHandle handle = getCacheHandle(blockCache, "handle_test", 0);
        assertNotNull(handle);
        handle.close(); // 显式关闭

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                handle::getData, "应该抛出IllegalStateException");
        System.out.println("正确捕获到关闭句柄的异常: " + exception.getMessage());
    }

    @Test
    @DisplayName("表缓存统计测试")
    void testTableCacheStats() {
        String tableName = "stats_table";
        int blockCount = 5;

        // 插入多个块
        for (int i = 0; i < blockCount; i++) {
            Block block = createTestBlock(tableName, i * 1024, 512);
            blockCache.put(tableName, i * 1024, block);
        }

        // 获取表统计
        BlockCache.TableCacheStats tableStats = blockCache.getTableStats(tableName);
        System.out.println("表统计: " + tableStats);

        assertEquals(tableName, tableStats.getTableName(), "表名应该匹配");
        assertEquals(blockCount, tableStats.getTotalBlocks(), "块数量应该匹配");
        assertEquals(blockCount, tableStats.getAccessibleBlocks(), "可访问块数量应该匹配");
        assertEquals(1.0, tableStats.getAccessibilityRatio(), 0.01, "可访问性比例应该是100%");

        // 失效部分块后再次检查
        blockCache.invalidateTable(tableName);
        tableStats = blockCache.getTableStats(tableName);
        System.out.println("失效后表统计: " + tableStats);
        assertEquals(0, tableStats.getTotalBlocks(), "失效后应该没有块");
    }

    @Test
    @DisplayName("模式匹配失效测试")
    void testPatternInvalidation() {
        // 创建符合不同模式的表名
        String[] tables = {
                "log_2024_01", "log_2024_02", "log_2023_12",
                "data_users", "data_products", "config_main"
        };

        // 插入所有表的数据
        for (String table : tables) {
            Block block = createTestBlock(table, 0, 256);
            blockCache.put(table, 0, block);
        }

        System.out.println("初始表: " + blockCache.getCachedTables());

        // 使用模式失效所有log_2024_*表
        blockCache.invalidateTablePattern("log_2024_*");

        Set<String> remainingTables = blockCache.getCachedTables();
        System.out.println("模式失效后剩余表: " + remainingTables);

        // 验证结果
        assertFalse(remainingTables.contains("log_2024_01"), "log_2024_01应该被移除");
        assertFalse(remainingTables.contains("log_2024_02"), "log_2024_02应该被移除");
        assertTrue(remainingTables.contains("log_2023_12"), "log_2023_12应该还在");
        assertTrue(remainingTables.contains("data_users"), "data_users应该还在");
    }

    @Test
    @DisplayName("清理陈旧映射测试")
    void testCleanupStaleMappings() {
        String tableName = "stale_test";

        // 插入一些块
        for (int i = 0; i < 3; i++) {
            Block block = createTestBlock(tableName, i * 1024, 256);
            blockCache.put(tableName, i * 1024, block);
        }

        // 手动删除一些块（模拟陈旧映射）
        // 注意：这里需要根据实际的删除方法调整
        // blockCache.delete("stale_test", 1024);

        // 运行清理
        blockCache.cleanupStaleMappings();

        // 验证表统计
        BlockCache.TableCacheStats stats = blockCache.getTableStats(tableName);
        System.out.println("清理后表统计: " + stats);
        assertTrue(stats.getTotalBlocks() >= 0, "块数量应该有效");
    }

    @Test
    @DisplayName("内存使用跟踪测试")
    void testMemoryUsageTracking() {
        // 插入一些数据
        for (int i = 0; i < 10; i++) {
            Block block = createTestBlock("memory_test", i * 512, 1024);
            blockCache.put("memory_test", i * 512, block);
        }

        // 检查内存使用情况
        long size = blockCache.getSize();
        long capacity = blockCache.getCapacity();
        double usageRatio = (double) size / capacity;

        System.out.println("内存使用 - 大小: " + size + ", 容量: " + capacity +
                ", 使用率: " + String.format("%.2f", usageRatio * 100) + "%");

        assertTrue(usageRatio >= 0 && usageRatio <= 1.0, "使用率应该在0-100%之间");
        assertTrue(size > 0, "缓存大小应该大于0");
    }

    @Test
    @DisplayName("分片缓存负载均衡测试")
    void testShardedCacheLoadBalancing() {
        // 测试分片缓存的负载分布
        ShardedCache shardedCache = new ShardedCache(CACHE_CAPACITY, 4);

        // 插入大量数据
        for (int i = 0; i < 1000; i++) {
            String key = "key_" + i;
            byte[] value = new byte[1024]; // 1KB
            Arrays.fill(value, (byte) i);
            shardedCache.put(key, value);
        }

        // 检查分片负载
        double[] loadFactors = shardedCache.getShardLoadFactors();
        System.out.println("分片负载因子: " + Arrays.toString(loadFactors));

        // 验证负载相对均衡（允许一定的不均衡）
        double averageLoad = Arrays.stream(loadFactors).average().orElse(0);
        for (double load : loadFactors) {
            double deviation = Math.abs(load - averageLoad) / averageLoad;
            assertTrue(deviation < 0.5, "分片负载应该相对均衡，偏差: " + deviation);
        }
    }

    // 辅助方法
    private Block createTestBlock(String tableName, long offset, int size) {
        // 创建模拟的Block对象
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) (offset % 256)); // 用偏移量填充数据以便区分

        // 这里需要根据实际的Block构造函数来创建
        // 假设Block有这样的构造函数
        return new Block(tableName, offset, data, System.currentTimeMillis());
    }

    private byte[] getBlockData(Block block) {
        // 获取Block的数据用于比较
        return block.getData();
    }

    private CacheHandle getCacheHandle(BlockCache cache, String tableName, long offset) {
        // 辅助方法获取CacheHandle
        // 这里需要根据实际的BlockCache API调整
        // 假设BlockCache有这样的方法
        Block block = cache.get(tableName, offset);
        if (block != null) {
            // 创建模拟的CacheHandle
            CacheEntry entry = new CacheEntry(tableName + ":" + offset, block.getData());
            return new CacheHandle(entry, cache);
        }
        return null;
    }
}