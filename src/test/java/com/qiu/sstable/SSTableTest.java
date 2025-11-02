package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SSTable 综合测试 (JUnit 5)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SSTableTest {

    @TempDir
    Path tempDir;

    private String testFilePath;
    private BytewiseComparator comparator;

    @BeforeAll
    void setUpStatic() {
        // 只初始化不依赖tempDir的资源
        comparator = new BytewiseComparator();
    }

    @BeforeEach
    void setUpEach() {
        // 依赖tempDir的初始化移到这里，因为@BeforeEach在@TempDir注入后执行
        testFilePath = tempDir.resolve("test.sst").toString();
    }

    @AfterEach
    void cleanup() {
        // 每个测试后清理资源
    }

    @Test
    @DisplayName("详细调试文件格式")
    void detailedDebugFileFormat() throws IOException {
        String debugFile = tempDir.resolve("detailed_debug.sst").toString();

        System.out.println("=== 开始构建 ===");

        try (TableBuilder builder = new TableBuilder(debugFile, 4096, comparator, 10)) {
            System.out.println("添加数据...");
            builder.add("key1".getBytes(), "value1".getBytes());
            builder.add("key2".getBytes(), "value2".getBytes());

            System.out.println("完成构建...");
            builder.finish();

            System.out.println("构建统计: " + builder.getStats());
            System.out.println("最终文件大小: " + builder.getFileSize());
        }

        // 手动分析文件内容
        System.out.println("\n=== 分析文件内容 ===");
        byte[] fileData = java.nio.file.Files.readAllBytes(Path.of(debugFile));
        System.out.println("实际文件大小: " + fileData.length);

        // 正确计算Footer起始位置（40字节）
        int footerStart = fileData.length - Footer.ENCODED_LENGTH;
        System.out.println("Footer 应该从位置: " + footerStart + " (文件大小 " + fileData.length + " - Footer大小 " + Footer.ENCODED_LENGTH + ")");

        // 读取正确的40字节Footer
        ByteBuffer buffer = ByteBuffer.wrap(fileData, footerStart, Footer.ENCODED_LENGTH);

        System.out.println("Footer 40字节内容:");
        for (int i = 0; i < Footer.ENCODED_LENGTH / 8; i++) {
            long value = buffer.getLong();
            String description = getFooterFieldDescription(i, value);
            System.out.printf("  [%d] 0x%016x (%d) - %s%n", i * 8, value, value, description);
        }

        // 尝试正常读取
        System.out.println("\n=== 尝试读取 ===");
        try (SSTable table = new SSTable(debugFile, comparator)) {
            System.out.println("SSTable 创建成功!");

            Footer footer = table.getFooter();
            System.out.println("Footer解析结果: " + footer);
            System.out.println("文件统计: " + table.getStats());

            // 验证数据读取
            System.out.println("\n=== 数据验证 ===");
            assertTrue(table.get("key1".getBytes()).isPresent(), "key1 应该存在");
            assertEquals("value1", new String(table.get("key1".getBytes()).get()), "key1 值应该匹配");

            assertTrue(table.get("key2".getBytes()).isPresent(), "key2 应该存在");
            assertEquals("value2", new String(table.get("key2".getBytes()).get()), "key2 值应该匹配");

            assertFalse(table.get("key3".getBytes()).isPresent(), "key3 不应该存在");

            System.out.println("所有数据验证通过!");

        } catch (Exception e) {
            System.out.println("读取失败: " + e.getMessage());
            e.printStackTrace();
            fail("SSTable 创建或读取失败");
        }
    }

    /**
     * 获取Footer字段描述
     */
    private String getFooterFieldDescription(int index, long value) {
        switch (index) {
            case 0:
                return "metaIndexOffset - 元数据块偏移量";
            case 1:
                return "metaIndexSize - 元数据块大小";
            case 2:
                return "indexOffset - 索引块偏移量";
            case 3:
                return "indexSize - 索引块大小";
            case 4:
                boolean magicMatch = value == Footer.MAGIC_NUMBER;
                return String.format("magic - 魔数 %s (expected: 0x%016x)",
                        magicMatch ? "✓" : "✗", Footer.MAGIC_NUMBER);
            default:
                return "未知字段";
        }
    }

    @Test
    @DisplayName("测试基本读写操作")
    void testBasicReadWrite() throws IOException {
        // 构建 SSTable
        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            builder.add("apple".getBytes(), "red".getBytes());
            builder.add("banana".getBytes(), "yellow".getBytes());
            builder.add("cherry".getBytes(), "red".getBytes());
            builder.add("date".getBytes(), "brown".getBytes());
            builder.finish();
        }

        // 读取验证
        try (SSTable table = new SSTable(testFilePath, comparator)) {
            // 测试存在的数据
            assertEquals("red", new String(table.get("apple".getBytes()).get()));
            assertEquals("yellow", new String(table.get("banana".getBytes()).get()));
            assertEquals("red", new String(table.get("cherry".getBytes()).get()));
            assertEquals("brown", new String(table.get("date".getBytes()).get()));

            // 测试不存在的数据
            assertFalse(table.get("elderberry".getBytes()).isPresent());
            assertFalse(table.get("".getBytes()).isPresent());
        }
    }

    @Test
    @DisplayName("测试空表")
    void testEmptyTable() throws IOException {
        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            builder.finish(); // 空表
        }

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            assertFalse(table.get("any".getBytes()).isPresent());
            assertEquals(0, table.getStats().getDataBlockCount());
        }
    }

    @Test
    @DisplayName("测试单条大数据")
    void testSingleLargeEntry() throws IOException {
        byte[] largeKey = new byte[1024]; // 1KB key
        byte[] largeValue = new byte[2048]; // 2KB value
        Arrays.fill(largeKey, (byte) 'K');
        Arrays.fill(largeValue, (byte) 'V');

        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            builder.add(largeKey, largeValue);
            builder.finish();
        }

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            assertTrue(table.get(largeKey).isPresent());
            byte[] result = table.get(largeKey).get();
            assertArrayEquals(largeValue, result);
        }
    }

    @Test
    @DisplayName("测试多个数据块")
    void testMultipleBlocks() throws IOException {
        // 使用小块大小强制生成多个数据块
        try (TableBuilder builder = new TableBuilder(testFilePath, 256, comparator, 10)) {
            for (int i = 0; i < 100; i++) {
                String key = String.format("key%03d", i);
                String value = String.format("value%03d", i);
                builder.add(key.getBytes(), value.getBytes());
            }
            builder.finish();
            System.out.println("构建完成，总块数: " + builder.getStats().getTotalBlocks());
        }

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            SSTable.TableStats stats = table.getStats();
            System.out.println("SSTable统计 - 数据块数量: " + stats.getDataBlockCount() + ", 文件大小: " + stats.getFileSize());
            assertTrue(stats.getDataBlockCount() > 1, "应该生成多个数据块");

            // 先测试几个关键键
            testKey(table, "key000", "value000");
            testKey(table, "key050", "value050");
            testKey(table, "key099", "value099");

            // 然后验证所有键
            for (int i = 0; i < 100; i++) {
                String key = String.format("key%03d", i);
                String expectedValue = String.format("value%03d", i);
                Optional<byte[]> result = table.get(key.getBytes());
                if (!result.isPresent()) {
                    System.out.println("键丢失: " + key);
                    // 调试这个特定的键
                    debugKeyLookup(table, key.getBytes());
                }
                assertTrue(result.isPresent(), "Key should exist: " + key);
                assertEquals(expectedValue, new String(result.get()));
            }

            // 验证不存在的键
            assertFalse(table.get("key999".getBytes()).isPresent());

            System.out.println("多数据块测试通过");
        }
    }

    private void testKey(SSTable table, String key, String expectedValue) throws IOException {
        Optional<byte[]> result = table.get(key.getBytes());
        System.out.println("测试键 '" + key + "': " + (result.isPresent() ? "找到" : "未找到"));
        if (result.isPresent()) {
            String actualValue = new String(result.get());
            System.out.println("  期望值: '" + expectedValue + "', 实际值: '" + actualValue + "'");
            assertEquals(expectedValue, actualValue);
        }
    }

    private void debugKeyLookup(SSTable table, byte[] key) throws IOException {
        System.out.println("=== 调试键查找: " + new String(key) + " ===");

        // 手动遍历索引块
        Block.BlockIterator indexIter = table.getIndexBlock().iterator();
        indexIter.seekToFirst();

        int indexCount = 0;
        while (indexIter.isValid()) {
            byte[] indexKey = indexIter.key();
            byte[] handleData = indexIter.value();
            BlockHandle handle = table.decodeBlockHandle(handleData);

            System.out.println("索引条目 " + indexCount + ": key='" + new String(indexKey) +
                    "', block offset=" + handle.offset() + ", size=" + handle.size());

            // 检查这个数据块
            Block dataBlock = table.readBlock(handle);
            Block.BlockIterator dataIter = dataBlock.iterator();
            dataIter.seekToFirst();

            int dataCount = 0;
            while (dataIter.isValid()) {
                System.out.println("  数据条目 " + dataCount + ": key='" + new String(dataIter.key()) + "'");
                dataIter.next();
                dataCount++;
                if (dataCount > 5) break; // 只显示前几个
            }

            indexIter.next();
            indexCount++;
        }
    }

    @Test
    @DisplayName("测试键顺序验证")
    void testKeyOrderValidation() throws IOException {
        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            builder.add("banana".getBytes(), "yellow".getBytes());
            builder.add("apple".getBytes(), "red".getBytes()); // 乱序应该抛出异常

            fail("应该抛出 IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ascending order"));
        }
    }

    @Test
    @DisplayName("测试迭代器功能")
    void testIterator() throws IOException {
        String[] keys = {"apple", "banana", "cherry", "date"};
        String[] values = {"red", "yellow", "red", "brown"};

        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            for (int i = 0; i < keys.length; i++) {
                builder.add(keys[i].getBytes(), values[i].getBytes());
            }
            builder.finish();
        }

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            SSTable.TableIterator iterator = table.iterator();

            // 测试顺序迭代
            iterator.seekToFirst();
            for (int i = 0; i < keys.length; i++) {
                assertTrue(iterator.isValid(), "Iterator should be valid at position " + i);
                assertEquals(keys[i], new String(iterator.key()));
                assertEquals(values[i], new String(iterator.value()));
                iterator.next();
            }
            assertFalse(iterator.isValid(), "Iterator should be at end");

            // 测试 Seek 功能
            iterator.seek("cherry".getBytes());
            assertTrue(iterator.isValid());
            assertEquals("cherry", new String(iterator.key()));
            assertEquals("red", new String(iterator.value()));

            // 测试 Seek 不存在的键
            iterator.seek("elderberry".getBytes());
            assertFalse(iterator.isValid());
        }
    }

    @Test
    @DisplayName("测试布隆过滤器")
    void testBloomFilter() throws IOException {
        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            builder.add("apple".getBytes(), "red".getBytes());
            builder.add("banana".getBytes(), "yellow".getBytes());
            builder.finish();
        }

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            BloomFilter filter = table.getFilter();

            // 测试存在的键
            assertTrue(filter.mayContain("apple".getBytes()));
            assertTrue(filter.mayContain("banana".getBytes()));

            // 测试不存在的键（可能有假阳性，但概率应该很低）
            assertFalse(filter.mayContain("nonexistent".getBytes()));

            // 验证过滤器统计信息
            assertTrue(filter.getBitSize() > 0);
            assertTrue(filter.getHashCount() > 0);
            assertTrue(filter.getElementCount() > 0);
        }
    }

    @Test
    @DisplayName("测试边界键")
    void testBoundaryKeys() throws IOException {
        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            builder.add("".getBytes(), "empty_key".getBytes()); // 空键
            builder.add("a".getBytes(), "single_char".getBytes());
            builder.add("zzzzzzzzzz".getBytes(), "long_key".getBytes()); // 长键
            builder.finish();
        }

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            assertEquals("empty_key", new String(table.get("".getBytes()).get()));
            assertEquals("single_char", new String(table.get("a".getBytes()).get()));
            assertEquals("long_key", new String(table.get("zzzzzzzzzz".getBytes()).get()));
        }
    }

    @Test
    @DisplayName("测试重复构建")
    void testRepeatedBuilds() throws IOException {
        for (int buildCount = 0; buildCount < 3; buildCount++) {
            String currentFile = tempDir.resolve("test_" + buildCount + ".sst").toString();

            try (TableBuilder builder = new TableBuilder(currentFile, 4096, comparator, 10)) {
                for (int i = 0; i < 10; i++) {
                    String key = "key" + i + "_build" + buildCount;
                    String value = "value" + i + "_build" + buildCount;
                    builder.add(key.getBytes(), value.getBytes());
                }
                builder.finish();
            }

            try (SSTable table = new SSTable(currentFile, comparator)) {
                for (int i = 0; i < 10; i++) {
                    String key = "key" + i + "_build" + buildCount;
                    String expectedValue = "value" + i + "_build" + buildCount;
                    Optional<byte[]> result = table.get(key.getBytes());
                    assertTrue(result.isPresent());
                    assertEquals(expectedValue, new String(result.get()));
                }
            }
        }
    }

    @Test
    @DisplayName("测试统计信息")
    void testStatistics() throws IOException {
        TableBuilder.BuildStats finalStats;

        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            for (int i = 0; i < 50; i++) {
                String key = String.format("key%03d", i);
                String value = String.format("value%03d", i);
                builder.add(key.getBytes(), value.getBytes());
            }

            builder.finish();

            // 在 finish() 后获取最终统计
            finalStats = builder.getStats();
        }

        System.out.println("最终构建统计: " + finalStats);
        assertTrue(finalStats.getTotalEntries() > 0, "应该有条目");
        assertTrue(finalStats.getTotalBlocks() > 0, "应该有数据块"); // finish() 后肯定>0
        assertTrue(finalStats.getCurrentBlockEntries() >= 0, "当前块条目应该>=0");

        try (SSTable table = new SSTable(testFilePath, comparator)) {
            SSTable.TableStats tableStats = table.getStats();
            System.out.println("SSTable统计: " + tableStats);

            assertTrue(tableStats.getDataBlockCount() > 0, "应该有数据块");
            assertTrue(tableStats.getFileSize() > 0, "文件大小应该>0");
            assertTrue(tableStats.getFileSize() > 1000, "至少1KB");
        }
    }

    @Test
    @DisplayName("测试资源清理")
    void testResourceCleanup() throws IOException {
        TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10);
        builder.add("test".getBytes(), "value".getBytes());
        builder.finish(); // ✅ 先完成构建
        builder.close(); // 然后关闭

        SSTable table = new SSTable(testFilePath, comparator);
        table.close(); // 应该能正常关闭

        // 尝试重新打开
        try (SSTable table2 = new SSTable(testFilePath, comparator)) {
            assertTrue(table2.get("test".getBytes()).isPresent());
        }
    }

    @Test
    @DisplayName("测试异常情况")
    void testErrorConditions() {
        // 测试无效文件路径
        assertThrows(IOException.class, () -> {
            new SSTable("/invalid/path/that/does/not/exist.sst", comparator);
        });

        // 测试空键
        assertThrows(NullPointerException.class, () -> {
            try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
                builder.add(null, "value".getBytes());
            }
        });

        // 测试空值
        assertThrows(NullPointerException.class, () -> {
            try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
                builder.add("key".getBytes(), null);
            }
        });
    }

    @Test
    @DisplayName("性能测试: 大量数据")
    void testPerformanceWithLargeDataset() throws IOException {
        int recordCount = 1000;
        long startTime = System.currentTimeMillis();

        // 写入性能测试
        try (TableBuilder builder = new TableBuilder(testFilePath, 4096, comparator, 10)) {
            for (int i = 0; i < recordCount; i++) {
                String key = String.format("user:%08d:profile", i);
                String value = String.format("{\"id\":%d,\"name\":\"user%d\",\"age\":%d}",
                        i, i, i % 100);
                builder.add(key.getBytes(), value.getBytes());
            }
            builder.finish();
        }
        long writeTime = System.currentTimeMillis() - startTime;

        // 读取性能测试
        startTime = System.currentTimeMillis();
        try (SSTable table = new SSTable(testFilePath, comparator)) {
            for (int i = 0; i < recordCount; i++) {
                String key = String.format("user:%08d:profile", i);
                Optional<byte[]> result = table.get(key.getBytes());
                assertTrue(result.isPresent(), "Key should exist: " + key);
            }
        }
        long readTime = System.currentTimeMillis() - startTime;

        System.out.printf("Performance: %d records - Write: %dms, Read: %dms%n",
                recordCount, writeTime, readTime);

        // 基本的性能断言（根据实际情况调整）
        assertTrue(writeTime < 10000, "写入应该在一定时间内完成");
        assertTrue(readTime < 10000, "读取应该在一定时间内完成");
    }
}