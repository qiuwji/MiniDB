package com.qiu;

import com.qiu.cache.BlockCache;
import com.qiu.compaction.CompactionManager;
import com.qiu.core.*;
import com.qiu.iterator.DBIterator;
import com.qiu.memory.MemTable;
import com.qiu.sstable.TableBuilder;
import com.qiu.version.FileMetaData;
import com.qiu.version.VersionEdit;
import com.qiu.version.VersionSet;
import com.qiu.wal.WAL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MiniDB 完整实现
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

        // 初始化组件
        this.versionSet = new VersionSet(dbPath, this.options.getMaxLevels());
        this.blockCache = new BlockCache(this.options.getCacheSize());
        this.compactionManager = new CompactionManager(versionSet);

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

        // 恢复流程：先恢复目录中所有历史 WAL，再创建新的 WAL 供后续写入
        recoverExistingWALs();

        // 创建新的 WAL 文件供后续写入
        this.currentLogNumber = versionSet.getNextFileNumber();
        this.wal = new WAL(versionSet.getWALFileName(currentLogNumber));

        System.out.println("MiniDB opened successfully: " + dbPath);
        System.out.println("WAL file: " + wal.getFilePath());
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
        try {
            byte[] value = versionSet.current().get(key);
            if (value != null) {
                successfulGets.incrementAndGet();
            }
            return value;
        } catch (Exception e) {
            System.err.println("Error reading from SSTable: " + e.getMessage());
            return null;
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
    public Status write(WriteBatch batch) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(batch, "Write batch cannot be null");

        if (batch.isEmpty()) {
            return Status.OK;
        }

        // 为整个 batch 分配连续序号
        long startSeq = sequenceNumber.getAndAdd(batch.size());

        try {
            // [NEW] 将起始序列号保存到 WriteBatch 中，以便 WAL 序列化
            batch.setSequenceNumber(startSeq);

            // 先应用到 memTable（确保内存状态一致）
            applyBatchToMemTable(memTable, batch, startSeq);

            // 然后写入 WAL（确保持久化）
            wal.write(batch);
            wal.flush(); // 确保数据刷盘

            // 检查是否需要切换内存表
            if (memTable.approximateSize() > options.getMemtableSize()) {
                System.out.println("MemTable size " + memTable.approximateSize() +
                        " exceeds limit " + options.getMemtableSize() + ", switching...");
                switchMemTable();
            }

            return Status.OK;

        } catch (IOException e) {
            System.err.println("Write failed: " + e.getMessage());
            // 回滚内存状态？这里需要谨慎处理
            return Status.IO_ERROR;
        }
    }

    @Override
    public DBIterator iterator() throws IOException {
        checkNotClosed();
        return new DBIterator(versionSet.current(), memTable, immutableMemTable);
    }

    @Override
    public DBStats getStats() {
        long memtableSize = memTable.approximateSize();
        if (immutableMemTable != null) {
            memtableSize += immutableMemTable.approximateSize();
        }

        // 估算SSTable数量和数据大小
        long sstableCount = versionSet.current().getTotalFileCount();
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
     * 恢复数据库状态
     * [MODIFIED] 重构以使用 WAL 中恢复的原始序列号
     */
    private void recoverExistingWALs() throws IOException {
        java.nio.file.Path dir = java.nio.file.Path.of(dbPath);
        if (!java.nio.file.Files.exists(dir)) {
            java.nio.file.Files.createDirectories(dir);
            return;
        }

        var stream = java.nio.file.Files.list(dir);
        try {
            var files = stream
                    .filter(p -> p.getFileName().toString().matches("\\d+\\.log"))
                    .sorted((a, b) -> {
                        long na = Long.parseLong(a.getFileName().toString().replace(".log", ""));
                        long nb = Long.parseLong(b.getFileName().toString().replace(".log", ""));
                        return Long.compare(na, nb);
                    })
                    .toList();

            // [MODIFIED] 跟踪恢复的最大序列号
            long maxRecoveredSeq = -1;
            int recoveredBatches = 0;
            int recoveredRecords = 0;

            for (var p : files) {
                String path = p.toString();
                System.out.println("Recovering WAL file: " + path);
                try (WAL oldWal = new WAL(path)) {
                    var batches = oldWal.recover();
                    for (var batch : batches) {
                        if (batch != null && !batch.isEmpty()) {

                            // [MODIFIED] 从 batch 中获取原始序列号
                            long originalSeq = batch.getSequenceNumber();

                            if (originalSeq == -1) {
                                // 序列号无效 (可能是旧格式的WAL或已损坏)
                                System.err.println("Warning: Recovered batch with invalid sequence number (-1) from " + path + ". Skipping batch.");
                                continue;
                            }

                            // [MODIFIED] 使用原始序列号应用到 memTable
                            applyBatchToMemTable(memTable, batch, originalSeq);

                            // [MODIFIED] 更新 maxRecoveredSeq
                            long lastSeqInBatch = originalSeq + batch.size() - 1;
                            if (lastSeqInBatch > maxRecoveredSeq) {
                                maxRecoveredSeq = lastSeqInBatch;
                            }

                            recoveredBatches++;
                            recoveredRecords += batch.size();

                            System.out.printf("Recovered batch with %d operations, original seq: %d%n",
                                    batch.size(), originalSeq);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to recover WAL file " + path + ": " + e.getMessage());
                    // 不中断，继续尝试恢复其余日志
                }
            }

            // [MODIFIED] 将全局序列号计数器设置为恢复的最大值 + 1
            if (maxRecoveredSeq != -1) {
                sequenceNumber.set(maxRecoveredSeq + 1);
            }

            System.out.println("Recovered " + recoveredBatches + " write batches (" +
                    recoveredRecords + " records) from WAL files.");
            System.out.println("Next sequence number set to: " + sequenceNumber.get());

            // 如果恢复后内存表过大，立即切换
            if (memTable.approximateSize() > options.getMemtableSize()) {
                System.out.println("MemTable too large after recovery, switching...");
                switchMemTable();
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
                // 如果同一个 userKey 的后续版本，跳过
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
        return new DBStatus(
                !closed,
                memTable.size(),
                immutableMemTable != null ? immutableMemTable.size() : 0,
                backgroundCompactionScheduled,
                versionSet.getActiveVersionCount(),
                versionSet.current().getTotalFileCount()
        );
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