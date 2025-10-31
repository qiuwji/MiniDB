package com.qiu.compaction;


import com.qiu.version.Version;

/**
 * 压缩策略接口
 */
public interface CompactionStrategy {
    /**
     * 选择需要压缩的文件
     */
    Compaction pickCompaction(Version currentVersion);

    /**
     * 检查是否需要压缩
     */
    boolean needCompaction(Version currentVersion);

    /**
     * 获取压缩优先级（数值越小优先级越高）
     */
    int getPriority(Version currentVersion);

    /**
     * 估算压缩工作量
     */
    long estimateCompactionWork(Version currentVersion);
}

