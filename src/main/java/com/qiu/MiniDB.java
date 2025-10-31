package com.qiu;

import com.qiu.cache.BlockCache;
import com.qiu.compaction.CompactionManager;
import com.qiu.core.*;
import com.qiu.iterator.DBIterator;
import com.qiu.memory.MemTable;
import com.qiu.version.VersionSet;
import com.qiu.wal.WAL;

import java.io.IOException;
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

        // 初始化WAL - 使用WAL文件名
        long logNumber = versionSet.getNextFileNumber();
        this.wal = new WAL(versionSet.getWALFileName(logNumber));

        // 初始化统计
        this.sequenceNumber = new AtomicLong(0);
        this.totalPuts = new AtomicLong(0);
        this.totalGets = new AtomicLong(0);
        this.totalDeletes = new AtomicLong(0);
        this.successfulGets = new AtomicLong(0);
        this.closed = false;
        this.backgroundCompactionScheduled = false;

        // 恢复现有数据
        recover();

        System.out.println("MiniDB opened successfully: " + dbPath);
        System.out.println("WAL file: " + wal.getFilePath());
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

        // 首先检查内存表
        if (memTable != null) {
            byte[] value = memTable.get(key);
            if (value != null) {
                successfulGets.incrementAndGet();
                return value;
            }
        }

        // 检查不可变内存表
        if (immutableMemTable != null) {
            byte[] value = immutableMemTable.get(key);
            if (value != null) {
                successfulGets.incrementAndGet();
                return value;
            }
        }

        // 检查SSTable
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

    @Override
    public Status write(WriteBatch batch) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(batch, "Write batch cannot be null");

        if (batch.isEmpty()) {
            return Status.OK;
        }

        // 分配序列号
        long sequence = sequenceNumber.getAndIncrement();

        try {
            // 写入WAL
            wal.write(batch);

            // 应用到内存表
            memTable.applyWriteBatch(batch, sequence);

            // 检查是否需要切换内存表
            if (memTable.approximateSize() > options.getMemtableSize()) {
                switchMemTable();
            }

            // 触发后台压缩
            maybeScheduleCompaction();

            return Status.OK;

        } catch (IOException e) {
            System.err.println("Write failed: " + e.getMessage());
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

        // 如果有不可变内存表，等待压缩完成
        if (immutableMemTable != null) {
            // 简化实现：在实际系统中需要更复杂的同步机制
            System.out.println("Flush: waiting for compaction to complete...");
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

            // 等待压缩完成
            compactionManager.close();

            // 刷写最后的数据
            if (memTable.size() > 0) {
                switchMemTable();
            }

            // 关闭组件
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
     */
    private void recover() throws IOException {
        // 从WAL恢复未提交的写入
        var batches = wal.recover();
        if (!batches.isEmpty()) {
            System.out.println("Recovering " + batches.size() + " write batches from WAL");

            long sequence = sequenceNumber.get();
            for (var batch : batches) {
                memTable.applyWriteBatch(batch, sequence);
                sequence += batch.size();
            }
            sequenceNumber.set(sequence);

            // 如果恢复后内存表过大，立即切换
            if (memTable.approximateSize() > options.getMemtableSize()) {
                switchMemTable();
            }
        }
    }

    /**
     * 切换内存表
     */
    private void switchMemTable() throws IOException {
        if (immutableMemTable != null) {
            // 等待前一个不可变内存表被压缩
            System.out.println("Waiting for previous immutable memtable compaction...");
            return;
        }

        // 创建新的WAL文件
        long newLogNumber = versionSet.getNextFileNumber();
        WAL newWal = new WAL(versionSet.getWALFileName(newLogNumber));

        // 切换内存表
        immutableMemTable = memTable;
        memTable = new MemTable();

        // 切换WAL
        wal.close();
        wal = newWal;

        // 立即触发压缩
        maybeScheduleCompaction();

        System.out.println("MemTable switched, new WAL: " + versionSet.getWALFileName(newLogNumber));
    }

    /**
     * 可能调度后台压缩
     */
    private void maybeScheduleCompaction() {
        if (!backgroundCompactionScheduled) {
            backgroundCompactionScheduled = true;

            // 在实际实现中，这里会使用线程池或后台线程
            // 简化实现：直接请求压缩
            new Thread(() -> {
                try {
                    compactionManager.requestCompaction();
                    backgroundCompactionScheduled = false;
                } catch (Exception e) {
                    System.err.println("Background compaction failed: " + e.getMessage());
                    backgroundCompactionScheduled = false;
                }
            }, "BackgroundCompaction").start();
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
        if (key.length > 1024) { // 合理的键大小限制
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
        // 删除数据库目录及其内容
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