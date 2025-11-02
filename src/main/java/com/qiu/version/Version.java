package com.qiu.version;

import com.qiu.sstable.SSTable;
import com.qiu.util.BytewiseComparator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据库版本，表示某个时间点的数据库状态
 * (修复了线程安全和实现逻辑)
 */
public class Version {
    private final VersionSet versionSet;
    private final List<List<FileMetaData>> files; // 每个层级的文件列表
    private final long versionId;
    private final java.util.Comparator<byte[]> comparator;

    // === 修复 P3: 'refs' 必须是原子的，以实现线程安全的引用计数 ===
    private final AtomicInteger refs;

    public Version(VersionSet versionSet) {
        this.versionSet = Objects.requireNonNull(versionSet, "VersionSet cannot be null");
        this.comparator = new BytewiseComparator();
        this.files = new ArrayList<>();
        this.versionId = System.currentTimeMillis(); // 使用时间戳作为版本ID

        // === 修复 P3: 初始化 AtomicInteger ===
        this.refs = new AtomicInteger(0);

        // 初始化所有层级
        for (int i = 0; i < versionSet.getMaxLevels(); i++) {
            // === 修复 P5: 使用 ArrayList 替代 CopyOnWriteArrayList ===
            // 1. COWAL 在构建时 (add/remove) 效率极低。
            // 2. COWAL 的迭代器不支持 remove()，导致 removeFile() 必定失败。
            files.add(new ArrayList<>());
        }
    }

    /**
     * 查找指定键的值
     */
    public byte[] get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "Key cannot be null");

        // 从第0层开始查找（最新的数据）
        // === 修复 P5.1: getFiles() 现在返回防御性拷贝，可以安全迭代 ===
        List<FileMetaData> level0Files = getFiles(0);
        for (FileMetaData fileMeta : level0Files) {
            if (fileMeta.containsKey(key)) {
                byte[] value = lookupFile(fileMeta, key);
                if (value != null) {
                    return value;
                }
            }
        }

        // 查找 L1+ 层
        for (int level = 1; level < files.size(); level++) {
            List<FileMetaData> levelFiles = getFiles(level);

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

            // 比较 key 和 file.largest
            if (compareKeys(key, file.getLargestKey()) > 0) {
                // key 在 file 右侧
                left = mid + 1;
            }
            // 比较 key 和 file.smallest
            else if (compareKeys(key, file.getSmallestKey()) < 0) {
                // key 在 file 左侧
                right = mid - 1;
            } else {
                // key 在 [file.smallest, file.largest] 范围内
                return mid;
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

        // === 修复 P4 & P5.2: 修复 L0/L1+ 的添加逻辑 ===
        // get(key) 的实现表明 L0 文件是无序且可能重叠的。
        // 而 L1+ 是有序且不重叠的。

        if (level == 0) {
            // L0: 直接添加，不需要排序。
            levelFiles.add(file);
        } else {
            // L1+: 按 smallestKey 排序，使用二分查找 (P4 效率修复)
            int index = Collections.binarySearch(levelFiles, file,
                    (f1, f2) -> compareKeys(f1.getSmallestKey(), f2.getSmallestKey()));

            if (index < 0) {
                index = -(index + 1); // 计算插入点
            }
            levelFiles.add(index, file);
        }
    }

    /**
     * 从指定层级删除文件
     */
    public boolean removeFile(int level, long fileNumber) {
        if (level < 0 || level >= files.size()) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }

        List<FileMetaData> levelFiles = files.get(level);

        // === 修复 P5: 现在使用的是 ArrayList，it.remove() 可以正常工作 ===
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
     *
     * === 修复 P5.1: 公共 getter 必须返回拷贝，以保护内部列表 ===
     * 内部列表现在是 ArrayList，不是线程安全的。
     */
    public List<FileMetaData> getFiles(int level) {
        if (level < 0 || level >= files.size()) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }
        // 返回一个拷贝，供外部（主要是 'get' 方法）安全迭代
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
     * === 修复 P5.1: 内部方法，供 VersionSet.logAndApply *构建* 版本时使用 ===
     * 返回 *真实* 的列表以供修改。
     * 必须在 VersionSet 的 'synchronized (logAndApply)' 块中调用。
     */
    List<List<FileMetaData>> getInternalFilesForEdit() {
        return files;
    }

    /**
     * === 修复 P5.1: 内部方法，供 VersionSet *读取* 'current' 版本时使用 ===
     * 返回 *真实* 的列表以供复制。
     * 必须在 VersionSet 的 'synchronized (logAndApply)' 块中调用。
     */
    List<List<FileMetaData>> getInternalFilesForRead() {
        // 在这个设计中，一个已发布的 Version 是不可变的，
        // 但 logAndApply 需要复制它。返回 'files' 是最高效的。
        return files;
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
        // === 修复 P5: 'files.get(level)' 是 ArrayList, .size() 是 O(1) ===
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
     * === 修复 P3: 使用 AtomicInteger 实现线程安全 ===
     */
    public void ref() {
        refs.incrementAndGet();
    }

    /**
     * 减少引用计数，返回是否还有引用
     * === 修复 P3: 使用 AtomicInteger 实现线程安全 ===
     */
    public boolean unref() {
        int currentRefs = refs.decrementAndGet();
        if (currentRefs < 0) {
            // 如果发生这种情况，说明有双重释放，这是个严重的 bug
            throw new IllegalStateException("Reference count decreased below zero");
        }
        return currentRefs > 0;
    }

    /**
     * 获取引用计数
     * === 修复 P3: 使用 AtomicInteger 实现线程安全 ===
     */
    public int getRefCount() {
        return refs.get();
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
                versionId, refs.get(), getAllFiles());
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