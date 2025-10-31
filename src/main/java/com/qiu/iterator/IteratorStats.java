// src/main/java/com/qiu/iterator/IteratorStats.java
package com.qiu.iterator;

/**
 * 迭代器统计信息
 */
public class IteratorStats {
    private final int sstablesAccessed;
    private final int blocksRead;
    private final long startTime;
    private final long endTime;

    public IteratorStats(int sstablesAccessed, int blocksRead) {
        this.sstablesAccessed = sstablesAccessed;
        this.blocksRead = blocksRead;
        this.startTime = System.currentTimeMillis();
        this.endTime = startTime;
    }

    public IteratorStats(int sstablesAccessed, int blocksRead, long startTime, long endTime) {
        this.sstablesAccessed = sstablesAccessed;
        this.blocksRead = blocksRead;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public int getSstablesAccessed() { return sstablesAccessed; }
    public int getBlocksRead() { return blocksRead; }
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }
    public long getDuration() { return endTime - startTime; }
    public double getBlocksPerSSTable() { return sstablesAccessed > 0 ? (double) blocksRead / sstablesAccessed : 0.0; }

    @Override
    public String toString() {
        return String.format("IteratorStats{sstables=%d, blocks=%d, duration=%dms, blocksPerSSTable=%.2f}",
                sstablesAccessed, blocksRead, getDuration(), getBlocksPerSSTable());
    }
}
