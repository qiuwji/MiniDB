import com.qiu.core.MiniDB;
import com.qiu.core.Options;
import com.qiu.core.Status;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试 MiniDB 写入 -> 关闭 -> 重启 -> 读取 的持久化流程
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MiniDBPersistenceTest {
    private final Path dbPath = Paths.get("test_example_db");

    @BeforeAll
    public void beforeAll() throws IOException {
        // 确保干净环境
        if (Files.exists(dbPath)) {
            deleteRecursive(dbPath);
        }
        Files.createDirectories(dbPath);
    }

    @AfterAll
    public void afterAll() throws IOException {
        // 测试结束后清理
        if (Files.exists(dbPath)) {
            deleteRecursive(dbPath);
        }
    }

    @Test
    public void testInsertCloseReopenRecover() throws IOException {
        Options options = Options.builder()
                .memtableSize(4 * 1024 * 1024)
                .cacheSize(8 * 1024 * 1024)
                .createIfMissing(true)
                .build();

        // 1) 打开 DB，插入 100 条数据，并验证能立即读取到
        try (MiniDB db = MiniDB.open(dbPath.toString(), options)) {
            for (int i = 1; i <= 100; i++) {
                String k = "key" + i;
                String v = "value" + i;
                Status s = db.put(k.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8));
                // 断言写入成功（你的实现返回 Status.OK/Status.IO_ERROR）
                assertEquals(Status.OK, s, "put() 应返回 OK: i=" + i);

                // 立即读回检查
                byte[] got = db.get(k.getBytes(StandardCharsets.UTF_8));
                assertNotNull(got, "插入后立即读取不能为 null, key=" + k);
                assertEquals(v, new String(got, StandardCharsets.UTF_8), "插入后读取值不匹配, key=" + k);
            }
        } // 自动 close()

        // 2) 在关闭后，检查目录下是否存在文件（WAL 或 SST）
        List<String> filesAfterClose = listDbFiles(dbPath);
        System.out.println("Files after close: " + filesAfterClose);
        assertFalse(filesAfterClose.isEmpty(), "关闭后目录应包含 WAL/SST 等文件，若为空说明未写入任何持久化文件");

        // 3) 重新打开 DB，读取 100 条数据，验证恢复成功
        try (MiniDB reopened = MiniDB.open(dbPath.toString(), options)) {
            for (int i = 1; i <= 100; i++) {
                String k = "key" + i;
                byte[] got = reopened.get(k.getBytes(StandardCharsets.UTF_8));
                // 如果为 null，打印诊断信息以便排查
                if (got == null) {
                    System.err.println("FAILED to find key after reopen: " + k);
                    System.err.println("Current DB directory files: " + listDbFiles(dbPath));
                }
                assertNotNull(got, "重启后应能读取到已写入的数据, key=" + k);
                assertEquals("value" + i, new String(got, StandardCharsets.UTF_8),
                        "重启后读取值不匹配, key=" + k);
            }
        }
    }

    // 辅助：列出 dbPath 下文件（相对路径）
    private List<String> listDbFiles(Path path) throws IOException {
        if (!Files.exists(path) || !Files.isDirectory(path))
            return List.of();
        try (var stream = Files.list(path)) {
            return stream.map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.toList());
        }
    }

    // 递归删除目录
    private void deleteRecursive(Path path) throws IOException {
        if (!Files.exists(path))
            return;
        try (var walker = Files.walk(path)) {
            walker.sorted((a, b) -> b.compareTo(a)) // 先删除子文件、再目录
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to delete " + p + ": " + e.getMessage(), e);
                        }
                    });
        }
    }
}
