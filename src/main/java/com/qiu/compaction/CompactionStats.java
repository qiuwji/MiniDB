package com.qiu.compaction;

import java.util.List;

/**
 * 压缩统计信息（简化）
 */
public class CompactionStats {
    private final int level;
    private final int inputFiles;
    private final int outputFiles;
    private final long inputBytes;
    private final long outputBytes;
    private final long startTime;
    private final long endTime;

    public CompactionStats(int level, int inputFiles, int outputFiles, long inputBytes, long outputBytes) {
        this.level = level;
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.inputBytes = inputBytes;
        this.outputBytes = outputBytes;
        this.startTime = System.currentTimeMillis();
        this.endTime = startTime;
    }

    public CompactionStats(int level, int inputFiles, int outputFiles, long inputBytes, long outputBytes,
                           long startTime, long endTime) {
        this.level = level;
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.inputBytes = inputBytes;
        this.outputBytes = outputBytes;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    // Getters
    public int getLevel() { return level; }
    public int getInputFiles() { return inputFiles; }
    public int getOutputFiles() { return outputFiles; }
    public long getInputBytes() { return inputBytes; }
    public long getOutputBytes() { return outputBytes; }
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }
    public long getDuration() { return endTime - startTime; }

    public double getCompressionRatio() {
        return inputBytes > 0 ? (double) outputBytes / inputBytes : 0.0;
    }

    public long getSpaceSaved() {
        return inputBytes - outputBytes;
    }

    public static CompactionStats merge(List<CompactionStats> statsList) {
        if (statsList.isEmpty()) return new CompactionStats(-1,0,0,0,0);

        int level = statsList.get(0).level;
        int totalInputFiles = 0, totalOutputFiles = 0;
        long totalInputBytes = 0, totalOutputBytes = 0, minStart = Long.MAX_VALUE, maxEnd = 0;

        for (CompactionStats s : statsList) {
            totalInputFiles += s.inputFiles;
            totalOutputFiles += s.outputFiles;
            totalInputBytes += s.inputBytes;
            totalOutputBytes += s.outputBytes;
            minStart = Math.min(minStart, s.startTime);
            maxEnd = Math.max(maxEnd, s.endTime);
        }

        return new CompactionStats(level, totalInputFiles, totalOutputFiles, totalInputBytes, totalOutputBytes, minStart, maxEnd);
    }

    @Override
    public String toString() {
        return String.format("CompactionStats{L%d: inputs=%d(%d bytes) -> outputs=%d(%d bytes), ratio=%.2f, saved=%d}",
                level, inputFiles, inputBytes, outputFiles, outputBytes, getCompressionRatio(), getSpaceSaved());
    }
}
