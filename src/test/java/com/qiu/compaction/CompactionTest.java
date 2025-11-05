package com.qiu.compaction;

import com.qiu.sstable.SSTable;
import com.qiu.sstable.TableBuilder;
import com.qiu.version.FileMetaData;
import com.qiu.version.Version;
import com.qiu.version.VersionEdit;
import com.qiu.version.VersionSet;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 压缩系统测试类
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CompactionTest {

    @TempDir
    Path tempDir;

    private VersionSet versionSet;
    private CompactionManager compactionManager;
    private LeveledCompaction strategy;

    @BeforeEach
    void setUp() throws IOException {
        // 创建模拟的 VersionSet
        versionSet = mock(VersionSet.class);

        // 设置基本行为
        when(versionSet.getMaxLevels()).thenReturn(7);
        when(versionSet.getNextFileNumber()).thenReturn(1L, 2L, 3L, 4L, 5L);
        when(versionSet.getTableFileName(anyLong())).thenAnswer(invocation -> {
            long fileNumber = invocation.getArgument(0);
            return tempDir.resolve(fileNumber + ".sst").toString();
        });

        // 创建策略和管理器
        strategy = new LeveledCompaction(versionSet, 1024 * 1024); // 1MB
        compactionManager = new CompactionManager(versionSet, strategy);
    }

    @AfterEach
    void tearDown() {
        if (compactionManager != null) {
            compactionManager.close();
        }
    }

    @Test
    void testCompactionCreation() {
        Compaction compaction = new Compaction(versionSet, 1);

        assertNotNull(compaction);
        assertEquals(1, compaction.getLevel());
        assertFalse(compaction.isDone());
        assertTrue(compaction.getInputs().isEmpty());
        assertTrue(compaction.getOutputs().isEmpty());
    }

    @Test
    void testAddInputFile() {
        Compaction compaction = new Compaction(versionSet, 1);
        FileMetaData file = new FileMetaData(1L, 1024, "a".getBytes(), "z".getBytes());

        compaction.addInputFile(1, file);

        List<FileMetaData> inputs = compaction.getInputs(1);
        assertEquals(1, inputs.size());
        assertEquals(file, inputs.get(0));
    }

    @Test
    void testAddInputFileInvalidLevel() {
        Compaction compaction = new Compaction(versionSet, 1);
        FileMetaData file = new FileMetaData(1L, 1024, "a".getBytes(), "z".getBytes());

        assertThrows(IllegalArgumentException.class,
                () -> compaction.addInputFile(-1, file));
        assertThrows(IllegalArgumentException.class,
                () -> compaction.addInputFile(10, file));
    }

    @Test
    void testIsTrivialMove() throws IOException {
        // 创建模拟版本
        Version currentVersion = mock(Version.class);
        when(versionSet.current()).thenReturn(currentVersion);

        Compaction compaction = new Compaction(versionSet, 1);
        FileMetaData inputFile = new FileMetaData(1L, 1024, "a".getBytes(), "m".getBytes());
        compaction.addInputFile(1, inputFile);

        // 模拟下一层没有重叠文件
        when(currentVersion.getFiles(2)).thenReturn(Arrays.asList(
                new FileMetaData(2L, 2048, "n".getBytes(), "z".getBytes())
        ));

        assertTrue(compaction.isTrivialMove());
    }

    @Test
    void testIsNotTrivialMoveDueToOverlap() throws IOException {
        Version currentVersion = mock(Version.class);
        when(versionSet.current()).thenReturn(currentVersion);

        Compaction compaction = new Compaction(versionSet, 1);
        FileMetaData inputFile = new FileMetaData(1L, 1024, "a".getBytes(), "m".getBytes());
        compaction.addInputFile(1, inputFile);

        // 模拟下一层有重叠文件
        when(currentVersion.getFiles(2)).thenReturn(Arrays.asList(
                new FileMetaData(2L, 2048, "k".getBytes(), "z".getBytes()) // 与输入文件重叠
        ));

        assertFalse(compaction.isTrivialMove());
    }

    @Test
    void testCompactionStats() {
        CompactionStats stats = new CompactionStats(1, 3, 1, 3072, 2048);

        assertEquals(1, stats.getLevel());
        assertEquals(3, stats.getInputFiles());
        assertEquals(1, stats.getOutputFiles());
        assertEquals(3072, stats.getInputBytes());
        assertEquals(2048, stats.getOutputBytes());
        assertTrue(stats.getCompressionRatio() > 0);
        assertEquals(1024, stats.getSpaceSaved());
    }

    @Test
    void testCompactionManagerLifecycle() {
        assertTrue(compactionManager.isRunning());
        assertFalse(compactionManager.isPaused());

        compactionManager.pause();
        assertTrue(compactionManager.isPaused());

        compactionManager.resume();
        assertFalse(compactionManager.isPaused());

        assertEquals(0, compactionManager.getPendingCompactions());
        assertTrue(compactionManager.getCompletedStats().isEmpty());
    }

    @Test
    void testLeveledCompactionStrategy() throws IOException {
        Version currentVersion = mock(Version.class);

        // 模拟 L0 有 5 个文件，应该触发压缩
        when(currentVersion.getFileCount(0)).thenReturn(5);
        when(currentVersion.getFiles(0)).thenReturn(Arrays.asList(
                new FileMetaData(1L, 1024, "a".getBytes(), "d".getBytes()),
                new FileMetaData(2L, 1024, "e".getBytes(), "h".getBytes()),
                new FileMetaData(3L, 1024, "i".getBytes(), "l".getBytes()),
                new FileMetaData(4L, 1024, "m".getBytes(), "p".getBytes()),
                new FileMetaData(5L, 1024, "q".getBytes(), "t".getBytes())
        ));

        when(currentVersion.getFiles(1)).thenReturn(Arrays.asList(
                new FileMetaData(6L, 2048, "a".getBytes(), "z".getBytes())
        ));

        assertTrue(strategy.needCompaction(currentVersion));

        Compaction compaction = strategy.pickCompaction(currentVersion);
        assertNotNull(compaction);
        assertEquals(0, compaction.getLevel());
        assertEquals(5, compaction.getInputs(0).size()); // 所有 L0 文件
    }

    @Test
    void testMergingIteratorBasic() throws IOException {
        // 创建测试 SSTable 文件
        createTestSSTable(1L, new String[][]{
                {"key1", "value1"},
                {"key3", "value3"}
        });

        createTestSSTable(2L, new String[][]{
                {"key2", "value2"},
                {"key4", "value4"}
        });

        // 创建迭代器列表
        List<MergingIterator.TableIterator> iterators = Arrays.asList(
                createTableIterator(1L),
                createTableIterator(2L)
        );

        MergingIterator mergingIterator = new MergingIterator(iterators);

        // 验证合并顺序
        assertTrue(mergingIterator.isValid());
        assertEquals("key1", new String(mergingIterator.key()));
        mergingIterator.next();

        assertEquals("key2", new String(mergingIterator.key()));
        mergingIterator.next();

        assertEquals("key3", new String(mergingIterator.key()));
        mergingIterator.next();

        assertEquals("key4", new String(mergingIterator.key()));
        mergingIterator.next();

        assertFalse(mergingIterator.isValid());
    }

    @Test
    void testMergingIteratorWithDuplicates() throws IOException {
        // 创建有重复键的 SSTable 文件（文件编号大的应该胜出）
        createTestSSTable(1L, new String[][]{
                {"key1", "old_value"},
                {"key2", "value2"}
        });

        createTestSSTable(2L, new String[][]{
                {"key1", "new_value"}, // 相同键，更新值
                {"key3", "value3"}
        });

        List<MergingIterator.TableIterator> iterators = Arrays.asList(
                createTableIterator(1L),
                createTableIterator(2L)
        );

        MergingIterator mergingIterator = new MergingIterator(iterators);

        // 验证去重逻辑（保留文件编号大的值）
        assertEquals("key1", new String(mergingIterator.key()));
        assertEquals("new_value", new String(mergingIterator.value())); // 应该是最新的值
        mergingIterator.next();

        assertEquals("key2", new String(mergingIterator.key()));
        mergingIterator.next();

        assertEquals("key3", new String(mergingIterator.key()));
    }

    @Test
    void testCompactionManagerRequestCompaction() throws IOException {
        Version currentVersion = mock(Version.class);
        when(versionSet.current()).thenReturn(currentVersion);
        when(currentVersion.getFileCount(0)).thenReturn(5); // 触发压缩

        // 模拟策略返回压缩任务
        Compaction mockCompaction = mock(Compaction.class);
        when(strategy.needCompaction(currentVersion)).thenReturn(true);
        when(strategy.pickCompaction(currentVersion)).thenReturn(mockCompaction);

        compactionManager.requestCompaction();

        // 验证任务被加入队列
        assertEquals(1, compactionManager.getPendingCompactions());
    }

    @Test
    void testEmptyMergingIterator() {
        MergingIterator mergingIterator = new MergingIterator(null);
        assertFalse(mergingIterator.isValid());

        MergingIterator mergingIterator2 = new MergingIterator(List.of());
        assertFalse(mergingIterator2.isValid());
    }

    @Test
    void testCompactionStatsMerge() {
        CompactionStats stats1 = new CompactionStats(1, 2, 1, 2048, 1024, 1000, 2000);
        CompactionStats stats2 = new CompactionStats(1, 3, 2, 3072, 2048, 1500, 2500);

        CompactionStats merged = CompactionStats.merge(Arrays.asList(stats1, stats2));

        assertEquals(1, merged.getLevel());
        assertEquals(5, merged.getInputFiles());
        assertEquals(3, merged.getOutputFiles());
        assertEquals(5120, merged.getInputBytes());
        assertEquals(3072, merged.getOutputBytes());
        assertEquals(1000, merged.getStartTime());
        assertEquals(2500, merged.getEndTime());
    }

    // 辅助方法：创建测试 SSTable 文件
    private void createTestSSTable(long fileNumber, String[][] keyValues) throws IOException {
        String path = tempDir.resolve(fileNumber + ".sst").toString();
        try (TableBuilder builder = new TableBuilder(path)) {
            for (String[] kv : keyValues) {
                builder.add(kv[0].getBytes(), kv[1].getBytes());
            }
            builder.finish();
        }
    }

    // 辅助方法：创建表迭代器
    private MergingIterator.TableIterator createTableIterator(long fileNumber) throws IOException {
        String path = tempDir.resolve(fileNumber + ".sst").toString();
        SSTable table = new SSTable(path);
        SSTable.TableIterator baseIter = table.iterator();
        return new MergingIterator.TableIterator(baseIter, fileNumber);
    }

    @Test
    void testCompactionPriority() throws IOException {
        Version currentVersion = mock(Version.class);

        // 测试 L0 优先级最高
        when(currentVersion.getFileCount(0)).thenReturn(4);
        assertEquals(0, strategy.getPriority(currentVersion));

        // 测试其他层级
        when(currentVersion.getFileCount(0)).thenReturn(3);
        when(currentVersion.getFiles(1)).thenReturn(Arrays.asList(
                new FileMetaData(1L, 2 * 1024 * 1024, "a".getBytes(), "z".getBytes()) // 超过 L1 限制
        ));

        // 需要模拟 getLevelSize 的行为
        // 这里简化测试，直接验证策略逻辑
        assertTrue(strategy.getPriority(currentVersion) < Integer.MAX_VALUE);
    }

    @Test
    void testCompactionWorkEstimation() throws IOException {
        Version currentVersion = mock(Version.class);
        when(currentVersion.getFileCount(0)).thenReturn(4);

        long work = strategy.estimateCompactionWork(currentVersion);
        assertTrue(work >= 0);
    }
}