package com.qiu.core;

/**
 * 数据库统计信息
 */
public class DBStats {
    private final long totalPuts;
    private final long totalGets;
    private final long totalDeletes;
    private final long successfulGets;
    private final long memtableSize;
    private final long sstableCount;
    private final long totalDataSize;

    public DBStats(long totalPuts, long totalGets, long totalDeletes,
                   long successfulGets, long memtableSize, long sstableCount, long totalDataSize) {
        this.totalPuts = totalPuts;
        this.totalGets = totalGets;
        this.totalDeletes = totalDeletes;
        this.successfulGets = successfulGets;
        this.memtableSize = memtableSize;
        this.sstableCount = sstableCount;
        this.totalDataSize = totalDataSize;
    }

    // Getters
    public long getTotalPuts() { return totalPuts; }
    public long getTotalGets() { return totalGets; }
    public long getTotalDeletes() { return totalDeletes; }
    public long getSuccessfulGets() { return successfulGets; }
    public long getMemtableSize() { return memtableSize; }
    public long getSstableCount() { return sstableCount; }
    public long getTotalDataSize() { return totalDataSize; }

    /**
     * 获取读取命中率
     */
    public double getHitRate() {
        return totalGets > 0 ? (double) successfulGets / totalGets : 0.0;
    }

    @Override
    public String toString() {
        return String.format(
                "DBStats{puts=%d, gets=%d, deletes=%d, hitRate=%.3f, memtable=%d, sstables=%d, totalSize=%d}",
                totalPuts, totalGets, totalDeletes, getHitRate(), memtableSize, sstableCount, totalDataSize
        );
    }
}
