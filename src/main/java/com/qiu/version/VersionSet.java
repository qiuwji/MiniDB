// src/main/java/com/qiu/version/VersionSet.java
package com.qiu.version;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 版本管理器，维护数据库的所有版本
 */
public class VersionSet implements AutoCloseable {
    private final String dbPath;
    private final int maxLevels;
    private final AtomicLong nextFileNumber;
    private final AtomicLong manifestFileNumber;
    private final AtomicLong lastSequence;

    private Version current;
    private final List<Version> activeVersions;
    private final List<Version> obsoleteVersions;

    private Manifest manifest;

    public VersionSet(String dbPath) throws IOException {
        this(dbPath, 7); // 默认7个层级
    }

    public VersionSet(String dbPath, int maxLevels) throws IOException {
        this.dbPath = Objects.requireNonNull(dbPath, "Database path cannot be null");
        this.maxLevels = maxLevels;
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
        return current;
    }

    /**
     * 追加新版本
     */
    public void appendVersion(Version version) {
        Objects.requireNonNull(version, "Version cannot be null");

        // 取消当前版本的引用
        if (!current.unref()) {
            obsoleteVersions.add(current);
        }

        // 设置新版本为当前版本
        current = version;
        current.ref();
        activeVersions.add(version);

        // 清理过时版本
        cleanupObsoleteVersions();
    }

    /**
     * 记录版本编辑并应用到新版本
     */
    public void logAndApply(VersionEdit edit) throws IOException {
        Objects.requireNonNull(edit, "Version edit cannot be null");

        if (edit.isEmpty()) {
            return; // 空编辑不需要处理
        }

        // 创建新版本
        Version newVersion = new Version(this);

        // 复制当前版本的文件结构
        for (int level = 0; level < maxLevels; level++) {
            newVersion.getFiles(level).addAll(current.getFiles(level));
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

        // 写入Manifest
        manifest.writeEdit(edit);

        // 切换到新版本
        appendVersion(newVersion);
    }

    /**
     * 从Manifest恢复版本信息
     */
    public void recover() throws IOException {
        try {
            manifest.recover();
        } catch (IOException e) {
            System.err.println("Failed to recover from manifest: " + e.getMessage());
            // 创建新的Manifest文件
            manifest = new Manifest(getManifestFileName(), this);

            // 写入初始状态
            VersionEdit edit = new VersionEdit();
            edit.setComparatorName("leveldb.BytewiseComparator");
            edit.setNextFileNumber(nextFileNumber.get());
            edit.setLastSequence(lastSequence.get());
            manifest.writeEdit(edit);
        }
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
     */
    private void cleanupObsoleteVersions() {
        Iterator<Version> it = obsoleteVersions.iterator();
        while (it.hasNext()) {
            Version version = it.next();
            if (version.getRefCount() == 0) {
                // 可以安全删除该版本相关的文件
                cleanupVersionFiles(version);
                it.remove();
            }
        }
    }

    /**
     * 清理版本相关的文件
     */
    private void cleanupVersionFiles(Version version) {
        // 在实际实现中，这里会删除不再被任何版本引用的SSTable文件
        // 简化实现：只从内存中移除
        System.out.println("Cleaning up version: " + version.getVersionId());
    }

    /**
     * 关闭版本集
     */
    @Override
    public void close() throws IOException {
        if (manifest != null) {
            manifest.close();
        }

        // 清理所有活跃版本
        for (Version version : activeVersions) {
            while (version.unref()) {
                // 减少引用计数直到为0
            }
        }
        activeVersions.clear();

        // 清理过时版本
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