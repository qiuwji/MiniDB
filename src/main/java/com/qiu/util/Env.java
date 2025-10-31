package com.qiu.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * 环境相关的文件操作工具
 */
public class Env {
    private Env() {} // 防止实例化

    public static boolean fileExists(String path) {
        return Files.exists(Path.of(path));
    }

    public static void createDir(String path) throws IOException {
        Files.createDirectories(Path.of(path));
    }

    public static void deleteFile(String path) throws IOException {
        Files.deleteIfExists(Path.of(path));
    }

    public static String[] getChildren(String path) throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(path))) {
            return stream.map(p -> p.getFileName().toString())
                    .toArray(String[]::new);
        }
    }

    public static void renameFile(String oldPath, String newPath) throws IOException {
        Files.move(Path.of(oldPath), Path.of(newPath), StandardCopyOption.REPLACE_EXISTING);
    }

    public static long getFileSize(String path) throws IOException {
        return Files.size(Path.of(path));
    }
}
