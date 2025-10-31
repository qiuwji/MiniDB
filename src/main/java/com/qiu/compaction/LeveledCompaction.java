package com.qiu.compaction;

import com.qiu.version.FileMetaData;
import com.qiu.version.Version;
import com.qiu.version.VersionSet;

import java.util.List;
import java.util.Objects;

/**
 * 层级压缩策略（LevelDB风格，简化实现）
 */
public class LeveledCompaction implements CompactionStrategy {
    private final VersionSet versionSet;
    private final long maxFileSize;
    private final long[] levelMaxBytes;

    public LeveledCompaction(VersionSet versionSet) {
        this(versionSet, 2 * 1024 * 1024); // 默认2MB文件大小阈值
    }

    public LeveledCompaction(VersionSet versionSet, long maxFileSize) {
        this.versionSet = Objects.requireNonNull(versionSet, "VersionSet cannot be null");
        this.maxFileSize = maxFileSize;
        this.levelMaxBytes = setupLevelMaxBytes();
    }

    /**
     * 设置每个层级的最大字节数（指数或倍增增长，简化实现）
     */
    private long[] setupLevelMaxBytes() {
        int maxLevels = versionSet.getMaxLevels();
        long[] maxBytes = new long[maxLevels];

        // Level 0: 特殊处理（用文件数触发压缩）
        maxBytes[0] = 4 * maxFileSize;

        // Level 1+: 指数增长（这里用简单倍增）
        long baseBytes = maxFileSize;
        for (int level = 1; level < maxLevels; level++) {
            maxBytes[level] = baseBytes;
            baseBytes *= 10; // 每层增长 10 倍（可调）
        }

        return maxBytes;
    }

    @Override
    public Compaction pickCompaction(Version currentVersion) {
        Objects.requireNonNull(currentVersion, "Current version cannot be null");

        // 优先基于大小的压缩
        Compaction sizeCompaction = pickSizeCompaction(currentVersion);
        if (sizeCompaction != null) {
            return sizeCompaction;
        }

        // 其次基于查找次数（简化实现）
        Compaction seekCompaction = pickSeekCompaction(currentVersion);
        if (seekCompaction != null) {
            return seekCompaction;
        }

        return null;
    }

    @Override
    public boolean needCompaction(Version currentVersion) {
        return pickCompaction(currentVersion) != null;
    }

    @Override
    public int getPriority(Version currentVersion) {
        if (currentVersion.getFileCount(0) >= 4) {
            return 0;
        }

        for (int level = 1; level < versionSet.getMaxLevels(); level++) {
            long size = getLevelSize(currentVersion, level);
            if (size >= levelMaxBytes[level]) {
                return level;
            }
        }

        return Integer.MAX_VALUE;
    }

    @Override
    public long estimateCompactionWork(Version currentVersion) {
        Compaction c = pickCompaction(currentVersion);
        if (c == null) return 0L;
        long work = 0L;
        for (FileMetaData f : c.getInputs()) {
            work += f.getFileSize();
        }
        return work;
    }

    private Compaction pickSizeCompaction(Version currentVersion) {
        // Level 0 文件过多触发
        if (currentVersion.getFileCount(0) >= 4) {
            return createLevel0Compaction(currentVersion);
        }

        // 其它层级按大小触发
        for (int level = 1; level < versionSet.getMaxLevels(); level++) {
            long levelSize = getLevelSize(currentVersion, level);
            if (levelSize >= levelMaxBytes[level]) {
                return createSizeCompaction(currentVersion, level);
            }
        }

        return null;
    }

    private Compaction pickSeekCompaction(Version currentVersion) {
        // 简化：遍历找到 allowedSeeks <= 0 的文件
        for (int level = 0; level < versionSet.getMaxLevels(); level++) {
            List<FileMetaData> files = currentVersion.getFiles(level);
            for (FileMetaData f : files) {
                if (f.getAllowedSeeks() <= 0) {
                    return createSeekCompaction(currentVersion, level, f);
                }
            }
        }
        return null;
    }

    private Compaction createLevel0Compaction(Version currentVersion) {
        Compaction c = new Compaction(versionSet, 0);
        for (FileMetaData f : currentVersion.getFiles(0)) {
            c.addInputFile(0, f);
        }
        setupOtherInputs(c);
        return c;
    }

    private Compaction createSizeCompaction(Version currentVersion, int level) {
        Compaction c = new Compaction(versionSet, level);
        FileMetaData largest = findLargestFile(currentVersion.getFiles(level));
        if (largest != null) {
            c.addInputFile(level, largest);
            setupOtherInputs(c);
        }
        return c;
    }

    private Compaction createSeekCompaction(Version currentVersion, int level, FileMetaData file) {
        Compaction c = new Compaction(versionSet, level);
        c.addInputFile(level, file);
        setupOtherInputs(c);
        return c;
    }

    private void setupOtherInputs(Compaction compaction) {
        int level = compaction.getLevel();
        Version curr = versionSet.current();

        if (level == 0) {
            // 对 level0 的每个输入，找出与 level1 重叠的文件
            for (FileMetaData in : compaction.getInputs(0)) {
                for (FileMetaData L1 : curr.getFiles(1)) {
                    if (filesOverlap(in, L1)) {
                        compaction.addInputFile(1, L1);
                    }
                }
            }
        } else {
            int next = level + 1;
            if (next < versionSet.getMaxLevels()) {
                for (FileMetaData in : compaction.getInputs(level)) {
                    for (FileMetaData nf : curr.getFiles(next)) {
                        if (filesOverlap(in, nf)) {
                            compaction.addInputFile(next, nf);
                        }
                    }
                }
            }
        }
    }

    private boolean filesOverlap(FileMetaData a, FileMetaData b) {
        return !(compareKeys(a.getLargestKey(), b.getSmallestKey()) < 0 ||
                compareKeys(a.getSmallestKey(), b.getLargestKey()) > 0);
    }

    private FileMetaData findLargestFile(List<FileMetaData> files) {
        FileMetaData largest = null;
        for (FileMetaData f : files) {
            if (largest == null || f.getFileSize() > largest.getFileSize()) {
                largest = f;
            }
        }
        return largest;
    }

    private long getLevelSize(Version currentVersion, int level) {
        long s = 0;
        for (FileMetaData f : currentVersion.getFiles(level)) {
            s += f.getFileSize();
        }
        return s;
    }

    private int compareKeys(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int c = Byte.compare(a[i], b[i]);
            if (c != 0) return c;
        }
        return Integer.compare(a.length, b.length);
    }

    public long getMaxFileSize() { return maxFileSize; }

    public long getLevelMaxBytes(int level) {
        if (level < 0 || level >= levelMaxBytes.length) return 0;
        return levelMaxBytes[level];
    }
}
