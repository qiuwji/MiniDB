// src/main/java/com/github/minidb/version/Version.java
package com.qiu.version;

import com.qiu.sstable.SSTable;
import com.qiu.util.BytewiseComparator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 数据库版本，表示某个时间点的数据库状态
 */
public class Version {
    private final VersionSet versionSet;
    private final List<List<FileMetaData>> files; // 每个层级的文件列表
    private final long versionId;
    private final java.util.Comparator<byte[]> comparator;
    private int refs; // 引用计数

    public Version(VersionSet versionSet) {
        this.versionSet = Objects.requireNonNull(versionSet, "VersionSet cannot be null");
        this.comparator = new BytewiseComparator();
        this.files = new ArrayList<>();
        this.versionId = System.currentTimeMillis(); // 使用时间戳作为版本ID
        this.refs = 0;

        // 初始化所有层级
        for (int i = 0; i < versionSet.getMaxLevels(); i++) {
            files.add(new CopyOnWriteArrayList<>());
        }
    }

    /**
     * 查找指定键的值
     */
    public byte[] get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "Key cannot be null");

        // 从第0层开始查找（最新的数据）
        for (int level = 0; level < files.size(); level++) {
            List<FileMetaData> levelFiles = files.get(level);

            // 第0层文件可能有重叠，需要检查所有文件
            // 其他层级文件没有重叠，可以使用二分查找
            if (level == 0) {
                for (FileMetaData fileMeta : levelFiles) {
                    if (fileMeta.containsKey(key)) {
                        byte[] value = lookupFile(fileMeta, key);
                        if (value != null) {
                            return value;
                        }
                    }
                }
            } else {
                // 对于非0层，使用二分查找找到可能包含该键的文件
                int index = findFile(levelFiles, key);
                if (index >= 0) {
                    FileMetaData fileMeta = levelFiles.get(index);
                    byte[] value = lookupFile(fileMeta, key);
                    if (value != null) {
                        return value;
                    }
                }
            }
        }

        return null; // 未找到
    }

    /**
     * 在特定文件中查找键
     */
    private byte[] lookupFile(FileMetaData fileMeta, byte[] key) throws IOException {
        String fileName = versionSet.getTableFileName(fileMeta.getFileNumber());
        try (SSTable table = new SSTable(fileName, comparator)) {
            Optional<byte[]> value = table.get(key);
            return value.orElse(null);
        }
    }

    /**
     * 在有序文件列表中查找可能包含指定键的文件
     */
    private int findFile(List<FileMetaData> files, byte[] key) {
        int left = 0;
        int right = files.size() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            FileMetaData file = files.get(mid);

            int cmp = compareKeys(key, file.getLargestKey());
            if (cmp <= 0) {
                cmp = compareKeys(key, file.getSmallestKey());
                if (cmp >= 0) {
                    return mid; // 键在文件范围内
                } else {
                    right = mid - 1;
                }
            } else {
                left = mid + 1;
            }
        }

        return -1; // 未找到
    }

    /**
     * 添加文件到指定层级
     */
    public void addFile(int level, FileMetaData file) {
        if (level < 0 || level >= files.size()) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }
        Objects.requireNonNull(file, "File cannot be null");

        List<FileMetaData> levelFiles = files.get(level);

        // 保持文件有序（按最小键排序）
        int index = 0;
        while (index < levelFiles.size() &&
                compareKeys(levelFiles.get(index).getSmallestKey(), file.getSmallestKey()) < 0) {
            index++;
        }

        levelFiles.add(index, file);
    }

    /**
     * 从指定层级删除文件
     */
    public boolean removeFile(int level, long fileNumber) {
        if (level < 0 || level >= files.size()) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }

        List<FileMetaData> levelFiles = files.get(level);
        for (Iterator<FileMetaData> it = levelFiles.iterator(); it.hasNext(); ) {
            FileMetaData file = it.next();
            if (file.getFileNumber() == fileNumber) {
                it.remove();
                return true;
            }
        }

        return false;
    }

    /**
     * 获取指定层级的文件列表（防御性拷贝）
     */
    public List<FileMetaData> getFiles(int level) {
        if (level < 0 || level >= files.size()) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }
        return new ArrayList<>(files.get(level));
    }

    /**
     * 获取所有层级的文件（防御性拷贝）
     */
    public List<List<FileMetaData>> getAllFiles() {
        List<List<FileMetaData>> result = new ArrayList<>();
        for (List<FileMetaData> levelFiles : files) {
            result.add(new ArrayList<>(levelFiles));
        }
        return result;
    }

    /**
     * 获取层级数量
     */
    public int getLevelCount() {
        return files.size();
    }

    /**
     * 获取指定层级的文件数量
     */
    public int getFileCount(int level) {
        if (level < 0 || level >= files.size()) {
            return 0;
        }
        return files.get(level).size();
    }

    /**
     * 获取总文件数量
     */
    public int getTotalFileCount() {
        int count = 0;
        for (List<FileMetaData> levelFiles : files) {
            count += levelFiles.size();
        }
        return count;
    }

    /**
     * 增加引用计数
     */
    public void ref() {
        refs++;
    }

    /**
     * 减少引用计数，返回是否还有引用
     */
    public boolean unref() {
        if (refs <= 0) {
            throw new IllegalStateException("Reference count already zero");
        }
        refs--;
        return refs > 0;
    }

    /**
     * 获取引用计数
     */
    public int getRefCount() {
        return refs;
    }

    public long getVersionId() {
        return versionId;
    }

    public VersionSet getVersionSet() {
        return versionSet;
    }

    /**
     * 比较两个键
     */
    private int compareKeys(byte[] a, byte[] b) {
        return comparator.compare(a, b);
    }

    @Override
    public String toString() {
        return String.format("Version{id=%d, refs=%d, files=%s}",
                versionId, refs, getAllFiles());
    }

    /**
     * 获取某个层级的统计信息
     */
    public LevelStats getLevelStats(int level) {
        if (level < 0 || level >= files.size()) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }

        List<FileMetaData> levelFiles = files.get(level);
        long totalSize = 0;
        byte[] smallestKey = null;
        byte[] largestKey = null;

        for (FileMetaData file : levelFiles) {
            totalSize += file.getFileSize();
            if (smallestKey == null || compareKeys(file.getSmallestKey(), smallestKey) < 0) {
                smallestKey = file.getSmallestKey();
            }
            if (largestKey == null || compareKeys(file.getLargestKey(), largestKey) > 0) {
                largestKey = file.getLargestKey();
            }
        }

        return new LevelStats(level, levelFiles.size(), totalSize, smallestKey, largestKey);
    }

    /**
     * 层级统计信息
     */
    public static class LevelStats {
        private final int level;
        private final int fileCount;
        private final long totalSize;
        private final byte[] smallestKey;
        private final byte[] largestKey;

        public LevelStats(int level, int fileCount, long totalSize, byte[] smallestKey, byte[] largestKey) {
            this.level = level;
            this.fileCount = fileCount;
            this.totalSize = totalSize;
            this.smallestKey = smallestKey != null ? smallestKey.clone() : null;
            this.largestKey = largestKey != null ? largestKey.clone() : null;
        }

        // Getters
        public int getLevel() { return level; }
        public int getFileCount() { return fileCount; }
        public long getTotalSize() { return totalSize; }
        public byte[] getSmallestKey() { return smallestKey != null ? smallestKey.clone() : null; }
        public byte[] getLargestKey() { return largestKey != null ? largestKey.clone() : null; }

        @Override
        public String toString() {
            return String.format("LevelStats{level=%d, files=%d, size=%d}", level, fileCount, totalSize);
        }
    }
}
