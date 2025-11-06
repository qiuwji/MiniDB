package com.qiu.version;

import com.qiu.cache.BlockCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 版本管理器，维护数据库的所有版本
 * (修复了线程安全问题，添加安全文件删除功能)
 */
public class VersionSet implements AutoCloseable {
    private final String dbPath;
    private final int maxLevels;
    private final AtomicLong nextFileNumber;
    private final AtomicLong manifestFileNumber;
    private final AtomicLong lastSequence;

    // === 修复 P2: 'current' 必须是 volatile，以确保在线程间的可见性 ===
    private volatile Version current;
    private final List<Version> activeVersions;
    private final List<Version> obsoleteVersions;

    private Manifest manifest;

    // === 修改点: 增加 BlockCache 字段 ===
    private final BlockCache blockCache;

    // === 新增：安全文件删除机制 ===
    private final Set<Long> pendingDeletion = ConcurrentHashMap.newKeySet();

    // === 修改点: 链式调用到主构造函数 ===
    public VersionSet(String dbPath) throws IOException {
        this(dbPath, 7, null); // 默认7个层级, null cache
    }

    // === 修改点: 链式调用到主构造函数 ===
    public VersionSet(String dbPath, int maxLevels) throws IOException {
        this(dbPath, maxLevels, null); // 默认 null cache
    }

    /**
     * === 修改点: 新增的主构造函数，用于依赖注入 ===
     * * @param dbPath 数据库路径
     * @param maxLevels 最大层级
     * @param blockCache 注入的块缓存 (可以为 null)
     * @throws IOException
     */
    public VersionSet(String dbPath, int maxLevels, BlockCache blockCache) throws IOException {
        this.dbPath = Objects.requireNonNull(dbPath, "Database path cannot be null");
        this.maxLevels = maxLevels;
        this.blockCache = blockCache; // <-- 存储注入的缓存

        this.nextFileNumber = new AtomicLong(1);
        this.manifestFileNumber = new AtomicLong(0);
        this.lastSequence = new AtomicLong(0);

        this.activeVersions = new CopyOnWriteArrayList<>();
        this.obsoleteVersions = new CopyOnWriteArrayList<>();

        // 创建初始版本
        this.current = new Version(this);
        this.current.ref();
        this.activeVersions.add(current);

        // 初始化Manifest
        this.manifest = new Manifest(getManifestFileName(), this);

        // 尝试恢复现有版本
        recover();
    }


    /**
     * 获取当前版本
     */
    public Version current() {
        // === 修复 P3: 增加引用，防止 'current' 在使用时被清理 ===
        current.ref();
        return current;
    }

    /**
     * 追加新版本
     */
    public void appendVersion(Version version) {
        Objects.requireNonNull(version, "Version cannot be null");

        // 取消当前版本的引用
        // (unref() 现在是线程安全的)
        if (!current.unref()) {
            obsoleteVersions.add(current);
        }

        // 设置新版本为当前版本
        // ('current' 是 volatile, 写入操作对其他线程可见)
        current = version;
        current.ref();
        activeVersions.add(version);

        // 清理过时版本
        cleanupObsoleteVersions();
    }

    /**
     * 记录版本编辑并应用到新版本
     *
     * === 修复 P1: 此方法必须同步，以防止多个线程同时创建和应用版本 ===
     * 否则会导致竞态条件和版本丢失。
     */
    public synchronized void logAndApply(VersionEdit edit) throws IOException {
        Objects.requireNonNull(edit, "Version edit cannot be null");

        if (edit.isEmpty()) {
            return; // 空编辑不需要处理
        }

        // 创建新版本
        Version newVersion = new Version(this);

        List<List<FileMetaData>> currentFiles = current.getInternalFilesForRead();
        // 2. 获取 'newVersion' 的*真实*文件列表 (用于修改)
        List<List<FileMetaData>> newVersionFiles = newVersion.getInternalFilesForEdit();

        // 复制当前版本的文件结构
        for (int level = 0; level < maxLevels; level++) {
            // 从 'current' 复制到 'newVersion' 的真实列表
            newVersionFiles.get(level).addAll(currentFiles.get(level));
        }

        // 应用删除文件操作
        for (VersionEdit.DeletedFile deletedFile : edit.getDeletedFiles()) {
            // removeFile 现在作用于 'newVersion' 的真实列表
            newVersion.removeFile(deletedFile.getLevel(), deletedFile.getFileNumber());
        }

        // 应用新增文件操作
        for (VersionEdit.LevelFile newFile : edit.getNewFiles()) {
            // addFile 现在作用于 'newVersion' 的真实列表
            newVersion.addFile(newFile.getLevel(), newFile.getFile());
        }

        // 更新元数据
        if (edit.getNextFileNumber() != null) {
            nextFileNumber.set(edit.getNextFileNumber());
        }
        if (edit.getLastSequence() != null) {
            lastSequence.set(edit.getLastSequence());
        }

        // 写入Manifest
        manifest.writeEdit(edit);

        // === 关键修复：安全删除文件，排除 trivial move 的情况 ===
        for (VersionEdit.DeletedFile deletedFile : edit.getDeletedFiles()) {
            long fileNumber = deletedFile.getFileNumber();

            // 检查这个文件是否在新增文件列表中（trivial move 情况）
            if (!isFileInNewFiles(edit, fileNumber)) {
                safeDeleteFile(fileNumber);
            } else {
                System.out.println("Skipping deletion for trivial move file: " + fileNumber);
            }
        }

        // 切换到新版本
        appendVersion(newVersion);
    }

    /**
     * 检查文件是否在新增文件列表中（用于识别 trivial move）
     */
    private boolean isFileInNewFiles(VersionEdit edit, long fileNumber) {
        for (VersionEdit.LevelFile newFile : edit.getNewFiles()) {
            if (newFile.getFile().getFileNumber() == fileNumber) {
                return true; // 文件在删除列表也在新增列表 → trivial move
            }
        }
        return false;
    }

    /**
     * 安全删除文件 - 防止重复删除
     */
    public synchronized void safeDeleteFile(long fileNumber) {
        if (pendingDeletion.contains(fileNumber)) {
            System.out.println("File " + fileNumber + " already deleted or pending deletion, skipping");
            return;
        }

        try {
            String tableFileName = getTableFileName(fileNumber);
            Path filePath = Path.of(tableFileName);
            if (Files.exists(filePath)) {
                Files.delete(filePath);
                pendingDeletion.add(fileNumber);
                System.out.println("Safely deleted SSTable: " + filePath.getFileName());
            } else {
                System.out.println("File already deleted: " + filePath.getFileName());
                pendingDeletion.add(fileNumber);
            }
        } catch (IOException e) {
            System.err.println("Failed to delete file " + fileNumber + ": " + e.getMessage());
            // 即使删除失败也标记，避免重复尝试
            pendingDeletion.add(fileNumber);
        }
    }

    /**
     * 清理已删除文件记录
     */
    public void cleanupDeletedRecords() {
        pendingDeletion.clear();
        System.out.println("Cleared deleted files tracking");
    }

    /**
     * 从Manifest恢复版本信息
     */
    public void recover() throws IOException {
        try {
            manifest.recover();
        } catch (IOException e) {
            System.err.println("Failed to recover from manifest: " + e.getMessage());
            handleManifestCorruption();
        }
    }

    private void handleManifestCorruption() throws IOException {
        System.out.println("Handling manifest corruption...");

        // 扫描磁盘文件，找出最大的文件编号
        long maxFileNumber = findMaxFileNumberFromDisk();

        // 设置安全的文件编号（现有最大编号 + 1）
        nextFileNumber.set(maxFileNumber + 1);

        // 创建新的Manifest文件
        manifest = new Manifest(getManifestFileName(), this);

        // 重置序列号为0（更安全的选择）
        lastSequence.set(0);

        // 写入确定的初始状态
        VersionEdit edit = new VersionEdit();
        edit.setComparatorName("leveldb.BytewiseComparator");
        edit.setNextFileNumber(nextFileNumber.get());  // 现在这个值是安全的
        edit.setLastSequence(lastSequence.get());      // 明确的初始值
        manifest.writeEdit(edit);

        // 清空当前版本，从头开始
        current = new Version(this);
        current.ref();
        activeVersions.clear();
        activeVersions.add(current);

        System.out.println("Recovered from manifest corruption. Next file number: " + nextFileNumber.get());
    }

    /**
     * 扫描数据目录，找出最大的文件编号
     */
    private long findMaxFileNumberFromDisk() {
        long maxNumber = 0;
        try {
            java.nio.file.Path dir = java.nio.file.Path.of(dbPath);
            if (!java.nio.file.Files.exists(dir)) {
                return 0;
            }

            var files = java.nio.file.Files.list(dir).toList();
            for (var file : files) {
                String fileName = file.getFileName().toString();
                // 匹配 .sst 和 .log 文件
                if (fileName.matches("\\d+\\.(sst|log)")) {
                    try {
                        long number = Long.parseLong(fileName.split("\\.")[0]);
                        if (number > maxNumber) {
                            maxNumber = number;
                        }
                    } catch (NumberFormatException e) {
                        // 忽略格式不正确的文件名
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error scanning directory: " + e.getMessage());
        }
        return maxNumber;
    }

    /**
     * 获取下一个文件编号
     */
    public long getNextFileNumber() {
        return nextFileNumber.getAndIncrement();
    }

    /**
     * 获取最后序列号
     */
    public long getLastSequence() {
        return lastSequence.get();
    }

    /**
     * 设置最后序列号
     */
    public void setLastSequence(long sequence) {
        if (sequence < lastSequence.get()) {
            throw new IllegalArgumentException("Sequence number cannot decrease");
        }
        lastSequence.set(sequence);
    }

    /**
     * 获取WAL文件名
     */
    public String getWALFileName(long fileNumber) {
        return dbPath + "/" + String.format("%06d", fileNumber) + ".log";
    }

    /**
     * 获取表文件名
     */
    public String getTableFileName(long fileNumber) {
        return dbPath + "/" + String.format("%06d", fileNumber) + ".sst";
    }

    /**
     * 获取Manifest文件名
     */
    public String getManifestFileName() {
        return dbPath + "/MANIFEST-" + String.format("%06d", manifestFileNumber.get());
    }

    /**
     * 获取最大层级数
     */
    public int getMaxLevels() {
        return maxLevels;
    }

    /**
     * 获取数据库路径
     */
    public String getDbPath() {
        return dbPath;
    }

    /**
     * 清理过时版本
     *
     * 说明：原来使用迭代器并调用 remove() 会导致 CopyOnWriteArrayList 的迭代器抛 UnsupportedOperationException。
     * 这里使用 removeIf 并在 predicate 中先做文件清理，以保证在多线程环境下安全移除。
     */
    private void cleanupObsoleteVersions() {
        obsoleteVersions.removeIf(version -> {
            // === 修复 P3: getRefCount() 现在是线程安全的 ===
            if (version.getRefCount() == 0) {
                try {
                    cleanupVersionFiles(version);
                } catch (Exception e) {
                    // 捕获并记录异常，但仍返回 true 以便从集合中移除该版本，避免重复尝试造成阻塞
                    String id;
                    try {
                        id = String.valueOf(version.getVersionId());
                    } catch (Throwable t) {
                        id = version.toString();
                    }
                    System.err.println("Failed cleaning version " + id + ": " + e.getMessage());
                    e.printStackTrace();
                }
                return true;
            }
            return false;
        });
    }

    /**
     * 清理版本相关的文件
     */
    private void cleanupVersionFiles(Version version) {
        // 在实际实现中，这里会删除不再被任何版本引用的SSTable文件
        // 简化实现：只从内存中移除
        try {
            System.out.println("Cleaning up version: " + version.getVersionId());
        } catch (Throwable t) {
            System.out.println("Cleaning up version: " + version.toString());
        }
    }

    /**
     * 关闭版本集
     */
    @Override
    public void close() throws IOException {
        if (manifest != null) {
            manifest.close();
        }

        // 清理已删除文件记录
        cleanupDeletedRecords();

        // 清理所有活跃版本：只在 refCount > 0 时调用 unref，避免重复释放
        for (Version version : activeVersions) {
            try {
                // === 修复 P3: 循环 unref，直到计数为 0 ===
                while (version.getRefCount() > 0) {
                    try {
                        // unref() 现在是线程安全的
                        version.unref();
                    } catch (IllegalStateException e) {
                        System.err.println("Warning: version.unref() threw when closing: " + e.getMessage());
                        break;
                    }
                }
            } catch (Exception e) {
                // 记录但继续处理其它版本
                System.err.println("Error while unref-ing version during close: " + e.getMessage());
                e.printStackTrace();
            }
        }
        activeVersions.clear();

        // 清理过时版本（不再需要）
        obsoleteVersions.clear();
    }

    /**
     * 获取活跃版本数量
     */
    public int getActiveVersionCount() {
        return activeVersions.size();
    }

    /**
     * 获取过时版本数量
     */
    public int getObsoleteVersionCount() {
        return obsoleteVersions.size();
    }

    /**
     * 获取数据库统计信息
     */
    public DatabaseStats getDatabaseStats() {
        long totalFiles = 0;
        long totalSize = 0;

        for (int level = 0; level < maxLevels; level++) {
            List<FileMetaData> files = current.getFiles(level);
            totalFiles += files.size();
            for (FileMetaData file : files) {
                totalSize += file.getFileSize();
            }
        }

        return new DatabaseStats(totalFiles, totalSize, activeVersions.size(), obsoleteVersions.size());
    }

    /**
     * 从数据库目录恢复现有文件
     */
    public void recoverExistingFiles() throws IOException {
        if (!com.qiu.util.Env.fileExists(dbPath)) {
            return;
        }

        String[] files = com.qiu.util.Env.getChildren(dbPath);
        if (files == null) return;

        System.out.println("Recovering files from: " + dbPath);

        for (String file : files) {
            if (file.endsWith(".log")) {
                System.out.println("Found WAL file: " + file);
                // WAL文件在MiniDB.recover()中处理
            } else if (file.endsWith(".sst")) {
                System.out.println("Found SSTable file: " + file);
                // SSTable文件在Manifest恢复时处理
            } else if (file.startsWith("MANIFEST-")) {
                System.out.println("Found Manifest file: " + file);
                // Manifest文件已经在recover()中处理
            } else {
                System.out.println("Found unknown file: " + file);
            }
        }
    }

    /**
     * 应用版本编辑但不写入Manifest（专门用于恢复）
     */
    public void applyEditForRecovery(VersionEdit edit) throws IOException {
        Objects.requireNonNull(edit, "Version edit cannot be null");

        if (edit.isEmpty()) {
            return;
        }

        // 创建新版本
        Version newVersion = new Version(this);

        // === 修复 P5.1: 同 logAndApply, 确保正确复制 ===
        List<List<FileMetaData>> currentFiles = current.getInternalFilesForRead();
        List<List<FileMetaData>> newVersionFiles = newVersion.getInternalFilesForEdit();

        // 复制当前版本的文件结构
        for (int level = 0; level < maxLevels; level++) {
            newVersionFiles.get(level).addAll(currentFiles.get(level));
        }

        // 应用删除文件操作
        for (VersionEdit.DeletedFile deletedFile : edit.getDeletedFiles()) {
            newVersion.removeFile(deletedFile.getLevel(), deletedFile.getFileNumber());
        }

        // 应用新增文件操作
        for (VersionEdit.LevelFile newFile : edit.getNewFiles()) {
            newVersion.addFile(newFile.getLevel(), newFile.getFile());
        }

        // 更新元数据
        if (edit.getNextFileNumber() != null) {
            nextFileNumber.set(edit.getNextFileNumber());
        }
        if (edit.getLastSequence() != null) {
            lastSequence.set(edit.getLastSequence());
        }

        // ⚠️ 关键区别：不写入Manifest！
        // manifest.writeEdit(edit);  // 这行去掉

        // 切换到新版本
        appendVersion(newVersion);
    }

    /**
     * === 修改点: 实现 getBlockCache 方法 ===
     */
    public BlockCache getBlockCache() {
        return this.blockCache;
    }

    /**
     * 数据库统计信息
     */
    public static class DatabaseStats {
        private final long totalFiles;
        private final long totalSize;
        private final int activeVersions;
        private final int obsoleteVersions;

        public DatabaseStats(long totalFiles, long totalSize, int activeVersions, int obsoleteVersions) {
            this.totalFiles = totalFiles;
            this.totalSize = totalSize;
            this.activeVersions = activeVersions;
            this.obsoleteVersions = obsoleteVersions;
        }

        // Getters
        public long getTotalFiles() { return totalFiles; }
        public long getTotalSize() { return totalSize; }
        public int getActiveVersions() { return activeVersions; }
        public int getObsoleteVersions() { return obsoleteVersions; }

        @Override
        public String toString() {
            return String.format("DatabaseStats{files=%d, size=%d, activeVersions=%d, obsoleteVersions=%d}",
                    totalFiles, totalSize, activeVersions, obsoleteVersions);
        }
    }
}