package com.qiu;

import com.qiu.cache.BlockCache;
import com.qiu.compaction.CompactionManager;
import com.qiu.core.*;
import com.qiu.iterator.DBIterator;
import com.qiu.memory.MemTable;
import com.qiu.sstable.TableBuilder;
import com.qiu.version.FileMetaData;
import com.qiu.version.Version;
import com.qiu.version.VersionEdit;
import com.qiu.version.VersionSet;
import com.qiu.wal.WAL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MiniDB 完整实现
 * (修复了并发安全问题和版本引用计数问题)
 */
public class MiniDB implements DB {
    private final String dbPath;
    private final Options options;
    private final VersionSet versionSet;
    private final CompactionManager compactionManager;
    private final BlockCache blockCache;

    // 内存组件
    private MemTable memTable;
    private MemTable immutableMemTable;
    private WAL wal;
    private long currentLogNumber; // 当前WAL文件编号

    // 状态管理
    private final AtomicLong sequenceNumber;
    private final AtomicLong totalPuts;
    private final AtomicLong totalGets;
    private final AtomicLong totalDeletes;
    private final AtomicLong successfulGets;
    private boolean closed;
    private boolean backgroundCompactionScheduled;

    public MiniDB(String dbPath, Options options) throws IOException {
        this.dbPath = Objects.requireNonNull(dbPath, "Database path cannot be null");
        this.options = options != null ? options : Options.defaultOptions();

        // === 修改点: 依赖注入顺序 ===
        // 1. 首先创建 BlockCache
        this.blockCache = new BlockCache(this.options.getCacheSize());

        // 2. 将 BlockCache 注入到 VersionSet
        // (*** 假设 VersionSet 构造函数已修改为 (String, int, BlockCache) ***)
        this.versionSet = new VersionSet(dbPath, this.options.getMaxLevels(), this.blockCache);

        // 3. 继续创建其他组件
        this.compactionManager = new CompactionManager(versionSet);
        // === 结束修改 ===

        // 初始化内存表
        this.memTable = new MemTable();
        this.immutableMemTable = null;

        // 初始化统计
        this.sequenceNumber = new AtomicLong(0);
        this.totalPuts = new AtomicLong(0);
        this.totalGets = new AtomicLong(0);
        this.totalDeletes = new AtomicLong(0);
        this.successfulGets = new AtomicLong(0);
        this.closed = false;
        this.backgroundCompactionScheduled = false;

        // 恢复流程：恢复现有的WAL，但不自动创建新的
        recoverExistingWALs();

        // 只有在没有WAL文件时才创建新的
        if (this.wal == null) {
            this.currentLogNumber = versionSet.getNextFileNumber();
            this.wal = new WAL(versionSet.getWALFileName(currentLogNumber));
            System.out.println("Created new WAL file: " + wal.getFilePath());
        }

        System.out.println("MiniDB opened successfully: " + dbPath);
        System.out.println("Current WAL: " + (wal != null ? wal.getFilePath() : "None"));
        System.out.println("Options: " + options);
    }

    @Override
    public Status put(byte[] key, byte[] value) throws IOException {
        checkNotClosed();
        validateKey(key);
        Objects.requireNonNull(value, "Value cannot be null");

        // 创建写入批次
        WriteBatch batch = new WriteBatch();
        batch.put(key, value);

        Status status = write(batch);
        totalPuts.incrementAndGet();
        return status;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        checkNotClosed();
        validateKey(key);

        totalGets.incrementAndGet();

        // 首先检查内存表（active）
        if (memTable != null) {
            byte[] value = memTable.get(key);
            if (value != null) {
                successfulGets.incrementAndGet();
                return value;
            }
        }

        // 检查不可变内存表（immutable）
        if (immutableMemTable != null) {
            byte[] value = immutableMemTable.get(key);
            if (value != null) {
                successfulGets.incrementAndGet();
                return value;
            }
        }

        // 检查SSTable（通过 VersionSet.current()）
        Version currentVersion = null; // 声明在 try 块外部
        try {
            // 获取当前版本并增加引用计数
            currentVersion = versionSet.current();

            byte[] value = currentVersion.get(key);
            if (value != null) {
                successfulGets.incrementAndGet();
            }
            return value;
        } catch (Exception e) {
            System.err.println("Error reading from SSTable: " + e.getMessage());
            return null;
        } finally {
            if (currentVersion != null) {
                currentVersion.unref();
            }
        }
    }

    @Override
    public Status delete(byte[] key) throws IOException {
        checkNotClosed();
        validateKey(key);

        // 创建包含删除标记的写入批次
        WriteBatch batch = new WriteBatch();
        batch.delete(key);

        Status status = write(batch);
        totalDeletes.incrementAndGet();
        return status;
    }

    /**
     * 写入：为整个 batch 分配连续的起始序号
     */
    @Override
    public synchronized Status write(WriteBatch batch) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(batch, "Write batch cannot be null");

        if (batch.isEmpty()) {
            return Status.OK;
        }

        // 为整个 batch 分配连续序号
        long startSeq = sequenceNumber.getAndAdd(batch.size());

        try {
            // 将起始序列号保存到 WriteBatch 中，以便 WAL 序列化
            batch.setSequenceNumber(startSeq);

            // 写入 WAL（确保持久化）
            wal.write(batch);
            wal.flush(); // 确保数据刷盘

            // 应用到 memTable（确保内存状态一致）
            applyBatchToMemTable(memTable, batch, startSeq);

            // 检查是否需要切换内存表
            if (memTable.approximateSize() > options.getMemtableSize()) {
                System.out.println("MemTable size " + memTable.approximateSize() +
                        " exceeds limit " + options.getMemtableSize() + ", switching...");
                switchMemTable();
            }

            return Status.OK;

        } catch (IOException e) {
            System.err.println("Write failed: " + e.getMessage());
            return Status.IO_ERROR;
        }
    }

    @Override
    public DBIterator iterator() throws IOException {
        checkNotClosed();
        // 获取引用，迭代器内部会在关闭时 unref
        Version currentVersion = versionSet.current();
        return new DBIterator(currentVersion, memTable, immutableMemTable);
    }

    @Override
    public DBStats getStats() {
        long memtableSize = memTable.approximateSize();
        if (immutableMemTable != null) {
            memtableSize += immutableMemTable.approximateSize();
        }

        // 估算SSTable数量和数据大小
        // 为了安全获取 Version，这里使用 try-finally 确保 unref
        Version currentVersion = null;
        try {
            currentVersion = versionSet.current();
            long sstableCount = currentVersion.getTotalFileCount();
            long totalDataSize = versionSet.getDatabaseStats().getTotalSize();

            return new DBStats(
                    totalPuts.get(),
                    totalGets.get(),
                    totalDeletes.get(),
                    successfulGets.get(),
                    memtableSize,
                    sstableCount,
                    totalDataSize
            );
        } finally {
            if (currentVersion != null) {
                currentVersion.unref();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        checkNotClosed();

        // 强制WAL刷盘
        wal.flush();

        // 如果存在 immutable，阻塞等待其持久化
        if (immutableMemTable != null) {
            System.out.println("Flush: immutable memtable exists; attempting sync flush now...");
            flushImmutableNow();
        }
    }

    @Override
    public void compactRange(byte[] begin, byte[] end) throws IOException {
        checkNotClosed();

        System.out.printf("Manual compaction: %s - %s%n",
                begin != null ? new String(begin) : "begin",
                end != null ? new String(end) : "end");

        // 触发立即压缩
        compactionManager.compactNow();
    }

    @Override
    public Options getOptions() {
        return options;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;

            // 先触发并等待 compaction 线程关闭
            compactionManager.close();

            // 如果还有 immutable 没被持久化，尝试同步 flush
            if (immutableMemTable != null) {
                System.out.println("Closing: flushing remaining immutable memtable...");
                flushImmutableNow();
            }

            // 关闭 WAL 与版本集
            if (wal != null) {
                wal.close();
            }
            if (versionSet != null) {
                versionSet.close();
            }

            System.out.println("MiniDB closed: " + dbPath);
        }
    }

    /**
     * 恢复数据库状态 - 修复版：恢复现有的WAL，设置当前的wal引用
     */
    private void recoverExistingWALs() throws IOException {
        java.nio.file.Path dir = java.nio.file.Path.of(dbPath);
        if (!java.nio.file.Files.exists(dir)) {
            java.nio.file.Files.createDirectories(dir);
            return; // 新数据库，没有WAL文件
        }

        try (var stream = java.nio.file.Files.list(dir)) {
            // 找到编号最大的WAL文件（最新的）
            Optional<Path> latestWAL = stream
                    .filter(p -> p.getFileName().toString().matches("\\d+\\.log"))
                    .max((a, b) -> {
                        long na = Long.parseLong(a.getFileName().toString().replace(".log", ""));
                        long nb = Long.parseLong(b.getFileName().toString().replace(".log", ""));
                        return Long.compare(na, nb);
                    });

            if (latestWAL.isEmpty()) {
                System.out.println("No WAL files found for recovery.");
                return; // 没有WAL文件，让构造函数创建新的
            }

            String latestWalPath = latestWAL.get().toString();
            long walNumber = Long.parseLong(latestWAL.get().getFileName().toString().replace(".log", ""));

            System.out.println("Recovering from existing WAL file: " + latestWalPath);

            // [FIXED] 使用现有的WAL文件作为当前的wal
            this.currentLogNumber = walNumber;
            this.wal = new WAL(latestWalPath);

            long maxRecoveredSeq = -1;
            int recoveredBatches = 0;
            int recoveredRecords = 0;

            try {
                var batches = this.wal.recover(); // 使用当前的wal恢复
                for (var batch : batches) {
                    if (batch != null && !batch.isEmpty()) {
                        long originalSeq = batch.getSequenceNumber();

                        if (originalSeq == -1) {
                            System.err.println("Warning: Recovered batch with invalid sequence number from " + latestWalPath);
                            continue;
                        }

                        // 应用到memTable
                        applyBatchToMemTable(memTable, batch, originalSeq);

                        // 更新最大序列号
                        long lastSeqInBatch = originalSeq + batch.size() - 1;
                        if (lastSeqInBatch > maxRecoveredSeq) {
                            maxRecoveredSeq = lastSeqInBatch;
                        }

                        recoveredBatches++;
                        recoveredRecords += batch.size();

//                        System.out.printf("Recovered batch with %d operations, original seq: %d%n",
//                                batch.size(), originalSeq);
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed to recover WAL file " + latestWalPath + ": " + e.getMessage());
                // 恢复失败，关闭当前的wal，让构造函数创建新的
                if (this.wal != null) {
                    this.wal.close();
                    this.wal = null;
                }
                throw e;
            }

            // 设置全局序列号
            if (maxRecoveredSeq != -1) {
                sequenceNumber.set(maxRecoveredSeq + 1);
            }

            System.out.println("Recovery completed: " + recoveredBatches + " batches (" +
                    recoveredRecords + " records) recovered from " + latestWalPath);
            System.out.println("Next sequence number set to: " + sequenceNumber.get());

        }
    }

    /**
     * 删除比指定WAL文件更旧的文件
     */
    private void deleteOlderWALFiles(Path latestRecoveredWal) throws IOException {
        long recoveredNumber = Long.parseLong(latestRecoveredWal.getFileName().toString().replace(".log", ""));

        var stream = java.nio.file.Files.list(java.nio.file.Path.of(dbPath));
        try {
            var olderFiles = stream
                    .filter(p -> {
                        String fileName = p.getFileName().toString();
                        if (!fileName.matches("\\d+\\.log")) return false;
                        long fileNumber = Long.parseLong(fileName.replace(".log", ""));
                        return fileNumber < recoveredNumber;
                    })
                    .toList();

            for (var oldFile : olderFiles) {
                try {
                    java.nio.file.Files.delete(oldFile);
                    System.out.println("Deleted old WAL file: " + oldFile.getFileName());
                } catch (IOException e) {
                    System.err.println("Failed to delete old WAL: " + oldFile.getFileName());
                }
            }
        } finally {
            stream.close();
        }
    }

    /**
     * 切换内存表：将当前 memTable 变成 immutable，并创建新的 WAL
     */
    private synchronized void switchMemTable() throws IOException {
        // 如果已经有immutable memtable，先等待它被处理
        if (immutableMemTable != null) {
            System.out.println("Previous immutable memtable still exists, flushing now...");
            flushImmutableNow();
        }

        // 检查memTable是否为空
        if (memTable.isEmpty()) {
            System.out.println("Skip switching: current memtable is empty");
            return;
        }

        System.out.println("Switching memTable: " + memTable.size() + " entries, " +
                memTable.approximateSize() + " bytes");

        // 创建新的WAL文件
        long newLogNumber = versionSet.getNextFileNumber();
        WAL newWal = new WAL(versionSet.getWALFileName(newLogNumber));

        // 切换内存表
        immutableMemTable = memTable;
        memTable = new MemTable();

        // 切换WAL
        WAL oldWal = this.wal;
        this.wal = newWal;
        this.currentLogNumber = newLogNumber;

        // 关闭旧WAL（但不删除，因为数据还没持久化到SSTable）
        oldWal.close();

        System.out.println("MemTable switched: " +
                immutableMemTable.size() + " entries, new WAL: " +
                versionSet.getWALFileName(newLogNumber));

        // 立即触发后台压缩来处理immutable memtable
        maybeScheduleCompaction();
    }

    /**
     * 可能调度后台压缩
     */
    private synchronized void maybeScheduleCompaction() {
        if (!backgroundCompactionScheduled && immutableMemTable != null) {
            backgroundCompactionScheduled = true;

            new Thread(() -> {
                try {
                    System.out.println("Background compaction started...");
                    // 直接刷盘immutable memtable
                    flushImmutableNow();
                    // 然后触发SSTable压缩
                    compactionManager.requestCompaction();
                } catch (Exception e) {
                    System.err.println("Background compaction failed: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    backgroundCompactionScheduled = false;
                }
            }, "BackgroundCompaction").start();
        }
    }

    /**
     * 将 immutableMemTable 同步写成一个 SSTable 文件
     */
    private synchronized void flushImmutableNow() throws IOException {
        if (immutableMemTable == null) {
            return;
        }

        System.out.println("Flushing immutable memtable to SSTable...");
        System.out.println("Immutable memtable entries: " + immutableMemTable.size());

        // 生成新的 table file number & path
        long fileNumber = versionSet.getNextFileNumber();
        String tablePath = versionSet.getTableFileName(fileNumber);

        // 使用 TableBuilder 写入 SST
        TableBuilder builder = null;
        byte[] smallest = null;
        byte[] largest = null;
        int entriesWritten = 0;

        try {
            builder = new TableBuilder(tablePath);

            // 通过 MemTable 迭代器按 InternalKey 顺序遍历，取每个 userKey 的第一条（最新）记录写入 SST
            MemTable.MemTableIterator iter = immutableMemTable.iterator();
            byte[] lastUserKey = null;

            while (iter.isValid()) {
                byte[] userKey = iter.key().getUserKey();
                // 如果同一个 userKey 的后续版本，跳过（只保留最新版本）
                if (lastUserKey != null && java.util.Arrays.equals(userKey, lastUserKey)) {
                    iter.next();
                    continue;
                }

                byte[] value = iter.value();
                if (value != null) {
                    // 写入到 TableBuilder
                    builder.add(userKey, value);
                    entriesWritten++;

                    // 更新最小/最大键
                    if (smallest == null) {
                        smallest = userKey.clone();
                    }
                    largest = userKey.clone();
                }

                // 记住当前 userKey，跳过后续版本
                lastUserKey = userKey.clone();
                iter.next();
            }

            // 如果没有条目，直接关闭并删除文件（不应加入版本）
            if (entriesWritten == 0) {
                builder.close();
                // 删除空文件
                java.nio.file.Files.deleteIfExists(Path.of(tablePath));
                immutableMemTable = null;
                System.out.println("Flush skipped: immutable memtable has no valid entries.");
                return;
            }

            // 完成写入并关闭 builder
            builder.finish();
            long fileSize = builder.getFileSize();
            builder.close();

            // 构建 FileMetaData 并通过 VersionEdit 注册到 VersionSet（加入 L0）
            FileMetaData meta = new FileMetaData(fileNumber, fileSize,
                    smallest != null ? smallest : new byte[0],
                    largest != null ? largest : new byte[0]);

            VersionEdit edit = new VersionEdit();
            edit.addFile(0, meta);

            // 将 edit 写入 manifest 并切换版本
            versionSet.logAndApply(edit);

            System.out.println("Flushed immutable memtable to SST: " + tablePath +
                    " (size=" + fileSize + " bytes, entries=" + entriesWritten + ")");

            // 现在可以删除旧的WAL文件，因为数据已经持久化到SSTable
            deleteOldWALFiles();

        } catch (IOException e) {
            // 如果写表失败，尝试清理并抛出
            if (builder != null) {
                try { builder.close(); } catch (Exception ex) {}
            }
            // 删除可能未完成的文件
            try { java.nio.file.Files.deleteIfExists(Path.of(tablePath)); } catch (Exception ex) {}
            throw e;
        } finally {
            // 清理 immutable memtable 引用
            immutableMemTable = null;
        }
    }

    /**
     * 删除旧的WAL文件（数据已经持久化到SSTable）
     */
    private void deleteOldWALFiles() {
        try {
            java.nio.file.Path dir = java.nio.file.Path.of(dbPath);
            if (!java.nio.file.Files.exists(dir)) {
                return;
            }

            var stream = java.nio.file.Files.list(dir);
            try {
                var files = stream
                        .filter(p -> {
                            String fileName = p.getFileName().toString();
                            return fileName.matches("\\d+\\.log") &&
                                    !fileName.equals(String.format("%06d.log", currentLogNumber));
                        })
                        .toList();

                for (var file : files) {
                    try {
                        java.nio.file.Files.delete(file);
                        System.out.println("Deleted old WAL file: " + file.getFileName());
                    } catch (IOException e) {
                        System.err.println("Failed to delete old WAL file: " + file.getFileName());
                    }
                }
            } finally {
                stream.close();
            }
        } catch (IOException e) {
            System.err.println("Error cleaning up old WAL files: " + e.getMessage());
        }
    }

    /**
     * 把 WriteBatch 逐条应用到指定的 MemTable，按 op 顺序给每条操作分配序号 startSeq, startSeq+1, ...
     */
    private void applyBatchToMemTable(MemTable target, WriteBatch batch, long startSeq) {
        long seq = startSeq;
        for (WriteBatch.WriteOp op : batch.getOperations()) {
            if (op.isDelete) {
                target.delete(op.key, seq);
            } else {
                target.put(op.key, op.value, seq, com.qiu.memory.InternalKey.ValueType.VALUE);
            }
            seq++;
        }
    }

    /**
     * 验证键的有效性
     */
    private void validateKey(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (key.length > 1024) {
            throw new IllegalArgumentException("Key too large: " + key.length + " bytes");
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
    }

    public String getDbPath() {
        return dbPath;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * 获取数据库状态信息
     */
    public DBStatus getStatus() {
        // 为了安全获取 Version，这里使用 try-finally 确保 unref
        Version currentVersion = null;
        try {
            currentVersion = versionSet.current();
            return new DBStatus(
                    !closed,
                    memTable.size(),
                    immutableMemTable != null ? immutableMemTable.size() : 0,
                    backgroundCompactionScheduled,
                    versionSet.getActiveVersionCount(),
                    currentVersion.getTotalFileCount()
            );
        } finally {
            if (currentVersion != null) {
                currentVersion.unref();
            }
        }
    }

    /**
     * 数据库状态信息类
     */
    public static class DBStatus {
        private final boolean isOpen;
        private final long memTableSize;
        private final long immutableMemTableSize;
        private final boolean compactionRunning;
        private final int activeVersions;
        private final int totalSSTFiles;

        public DBStatus(boolean isOpen, long memTableSize, long immutableMemTableSize,
                        boolean compactionRunning, int activeVersions, int totalSSTFiles) {
            this.isOpen = isOpen;
            this.memTableSize = memTableSize;
            this.immutableMemTableSize = immutableMemTableSize;
            this.compactionRunning = compactionRunning;
            this.activeVersions = activeVersions;
            this.totalSSTFiles = totalSSTFiles;
        }

        public boolean isOpen() { return isOpen; }
        public long getMemTableSize() { return memTableSize; }
        public long getImmutableMemTableSize() { return immutableMemTableSize; }
        public boolean isCompactionRunning() { return compactionRunning; }
        public int getActiveVersions() { return activeVersions; }
        public int getTotalSSTFiles() { return totalSSTFiles; }

        @Override
        public String toString() {
            return String.format(
                    "DBStatus{open=%s, memTable=%d, immutableMemTable=%d, compaction=%s, versions=%d, sstFiles=%d}",
                    isOpen, memTableSize, immutableMemTableSize, compactionRunning, activeVersions, totalSSTFiles
            );
        }
    }

    /**
     * 打开数据库的静态方法
     */
    public static MiniDB open(String path, Options options) throws IOException {
        return new MiniDB(path, options);
    }

    /**
     * 打开数据库（使用默认选项）
     */
    public static MiniDB open(String path) throws IOException {
        return open(path, Options.defaultOptions());
    }

    /**
     * 销毁数据库
     */
    public static void destroy(String path) throws IOException {
        java.nio.file.Path dbPath = java.nio.file.Path.of(path);
        if (java.nio.file.Files.exists(dbPath)) {
            java.nio.file.Files.walk(dbPath)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.delete(p);
                            System.out.println("Deleted: " + p);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + p + " - " + e.getMessage());
                        }
                    });
            System.out.println("Database destroyed: " + path);
        } else {
            System.out.println("Database directory does not exist: " + path);
        }
    }
}