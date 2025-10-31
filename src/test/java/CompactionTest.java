import com.qiu.compaction.*;
import com.qiu.version.FileMetaData;
import com.qiu.version.Version;
import com.qiu.version.VersionSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class CompactionTest {
    private String testDbPath;
    private VersionSet versionSet;
    private CompactionManager compactionManager;

    @BeforeEach
    public void setUp() throws IOException {
        testDbPath = "test_compaction_db";
        Files.createDirectories(Path.of(testDbPath));
        versionSet = new VersionSet(testDbPath);
        compactionManager = new CompactionManager(versionSet);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (compactionManager != null) {
            compactionManager.close();
        }
        if (versionSet != null) {
            versionSet.close();
        }

        // 清理测试文件
        if (Files.exists(Path.of(testDbPath))) {
            Files.walk(Path.of(testDbPath))
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    @Test
    public void testCompactionStrategy() throws IOException {
        LeveledCompaction strategy = new LeveledCompaction(versionSet);
        Version currentVersion = versionSet.current();

        // 初始状态下不应该需要压缩
        assertFalse(strategy.needCompaction(currentVersion));
        assertNull(strategy.pickCompaction(currentVersion));

        // 测试优先级计算
        assertEquals(Integer.MAX_VALUE, strategy.getPriority(currentVersion));
    }

    @Test
    public void testCompactionCreation() throws IOException {
        Compaction compaction = new Compaction(versionSet, 0);

        assertEquals(0, compaction.getLevel());
        assertFalse(compaction.isDone());
        assertTrue(compaction.getInputs().isEmpty());
        assertTrue(compaction.getOutputs().isEmpty());
    }

    @Test
    public void testCompactionInputOutput() throws IOException {
        Compaction compaction = new Compaction(versionSet, 1);

        FileMetaData file1 = new FileMetaData(1, 1024, "a".getBytes(), "c".getBytes());
        FileMetaData file2 = new FileMetaData(2, 2048, "d".getBytes(), "f".getBytes());

        compaction.addInputFile(1, file1);
        compaction.addInputFile(1, file2);

        List<FileMetaData> inputs = compaction.getInputs();
        assertEquals(2, inputs.size());
        assertTrue(inputs.contains(file1));
        assertTrue(inputs.contains(file2));

        List<FileMetaData> level1Inputs = compaction.getInputs(1);
        assertEquals(2, level1Inputs.size());
    }

    @Test
    public void testTrivialMoveDetection() throws IOException {
        Compaction compaction = new Compaction(versionSet, 1);

        // 单个文件且没有重叠时应该是简单移动
        FileMetaData file = new FileMetaData(1, 1024, "a".getBytes(), "c".getBytes());
        compaction.addInputFile(1, file);

        // 注意：实际测试需要设置下一层没有重叠文件
        // 这里只是测试方法调用
        compaction.isTrivialMove(); // 应该不抛异常
    }

    @Test
    public void testCompactionStats() {
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
    public void testCompactionManager() throws IOException {
        assertTrue(compactionManager.isRunning());
        assertFalse(compactionManager.isPaused());
        assertEquals(0, compactionManager.getPendingCompactions());

        // 测试暂停和恢复
        compactionManager.pause();
        assertTrue(compactionManager.isPaused());

        compactionManager.resume();
        assertFalse(compactionManager.isPaused());

        // 测试同步压缩（应该返回false，因为没有需要压缩的）
        assertFalse(compactionManager.compactNow());
    }

    @Test
    public void testMergingIterator() throws IOException {
        // 简化测试：创建空的合并迭代器
        MergingIterator iterator = new MergingIterator(java.util.Collections.emptyList());
        assertFalse(iterator.isValid());

        // 测试无效状态下的操作
        try {
            iterator.key();
            fail("Should throw exception when invalid");
        } catch (IllegalStateException e) {
            // 预期异常
        }
    }

    @Test
    public void testLevelMaxBytesCalculation() {
        LeveledCompaction strategy = new LeveledCompaction(versionSet);

        // 测试层级大小限制计算
        assertTrue(strategy.getLevelMaxBytes(0) > 0);
        assertTrue(strategy.getLevelMaxBytes(1) > 0);
        assertTrue(strategy.getLevelMaxBytes(2) > strategy.getLevelMaxBytes(1));
    }

    @Test
    public void testCompactionPriority() throws IOException {
        LeveledCompaction strategy = new LeveledCompaction(versionSet);
        Version currentVersion = versionSet.current();

        // 添加一些文件到Level 0来测试优先级
        // 注意：这里只是测试接口，不实际触发压缩
        int priority = strategy.getPriority(currentVersion);
        assertTrue(priority >= 0);
    }
}