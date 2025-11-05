package com.qiu.compaction;

import com.qiu.version.Version;
import com.qiu.version.VersionSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 压缩管理器，负责调度和执行压缩任务
 */
public class CompactionManager implements AutoCloseable {

    private final VersionSet versionSet;
    private final CompactionStrategy strategy;
    private final BlockingQueue<Compaction> compactionQueue;
    private final List<CompactionStats> completedStats;
    private final Thread compactionThread;
    private final AtomicBoolean running;
    private final AtomicBoolean paused;

    public CompactionManager(VersionSet versionSet) {
        this(versionSet, new LeveledCompaction(versionSet));
    }

    public CompactionManager(VersionSet versionSet, CompactionStrategy strategy) {
        this.versionSet = Objects.requireNonNull(versionSet, "VersionSet cannot be null");
        this.strategy = Objects.requireNonNull(strategy, "Strategy cannot be null");
        this.compactionQueue = new LinkedBlockingQueue<>();
        this.completedStats = new ArrayList<>();
        this.running = new AtomicBoolean(true);
        this.paused = new AtomicBoolean(false);

        // 启动压缩线程
        this.compactionThread = new Thread(this::compactionWorker, "CompactionWorker");
        this.compactionThread.setDaemon(true);
        this.compactionThread.start();
    }

    /**
     * 请求压缩（异步）
     */
    public void requestCompaction() {
        if (!running.get() || paused.get()) {
            return;
        }

        Version currentVersion = versionSet.current();
        if (strategy.needCompaction(currentVersion)) {
            Compaction compaction = strategy.pickCompaction(currentVersion);
            if (compaction != null) {
                try {
                    compactionQueue.put(compaction);
                    System.out.println("Compaction requested: " + compaction);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 立即执行压缩（同步）
     */
    public boolean compactNow() throws IOException {
        Version currentVersion = versionSet.current();
        if (!strategy.needCompaction(currentVersion)) {
            return false;
        }

        Compaction compaction = strategy.pickCompaction(currentVersion);
        if (compaction != null) {
            executeCompaction(compaction);
            return true;
        }

        return false;
    }

    /**
     * 暂停压缩
     */
    public void pause() {
        paused.set(true);
    }

    /**
     * 恢复压缩
     */
    public void resume() {
        paused.set(false);
        // 注意：这里不需要手动 requestCompaction()，
        // worker 线程会自动解除阻塞并继续处理队列，
        // 并且在 executeCompaction 结束时会触发 requestCompaction。
        // 如果希望在 resume 时立即检查，可以保留：
        // requestCompaction();
    }

    /**
     * 获取待处理压缩数量
     */
    public int getPendingCompactions() {
        return compactionQueue.size();
    }

    /**
     * 获取完成的压缩统计
     */
    public List<CompactionStats> getCompletedStats() {
        return new ArrayList<>(completedStats);
    }

    /**
     * 获取总压缩统计
     */
    public CompactionStats getTotalStats() {
        return CompactionStats.merge(completedStats);
    }

    /**
     * 估算总压缩工作量
     */
    public long estimateTotalWork() {
        return strategy.estimateCompactionWork(versionSet.current());
    }

    @Override
    public void close() {
        running.set(false);
        compactionThread.interrupt();

        try {
            compactionThread.join(5000); // 等待5秒
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 压缩工作线程
     */
    private void compactionWorker() {
        while (running.get()) {
            try {
                // <-- 修改点 1: 修复暂停逻辑 -->
                // 在 take() 之前检查暂停状态
                while (paused.get() && running.get()) {
                    try {
                        // 处于暂停状态，循环等待
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        if (!running.get()) break; // 响应关闭
                        Thread.currentThread().interrupt();
                    }
                }

                // 如果在暂停时被关闭，则退出
                if (!running.get()) {
                    break;
                }
                // <-- 结束修改点 1 -->

                // 现在安全地拿任务
                Compaction compaction = compactionQueue.take();

                // (旧的 "if (paused.get()) { put() ... }" 逻辑已移除)

                executeCompaction(compaction);

            } catch (InterruptedException e) {
                if (running.get()) {
                    // 如果不是因为关闭而中断，继续运行
                    Thread.currentThread().interrupt();
                }
                break;
            } catch (Exception e) {
                System.err.println("Compaction failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 执行压缩任务
     */
    private void executeCompaction(Compaction compaction) {
        try {
            System.out.println("Starting compaction: " + compaction);

            long startTime = System.currentTimeMillis();
            compaction.run();
            long endTime = System.currentTimeMillis();

            // 记录统计信息
            CompactionStats stats = compaction.getStats();
            completedStats.add(stats);

            System.out.printf("Compaction completed in %d ms: %s%n",
                    (endTime - startTime), stats);

            // 检查是否还需要更多压缩
            requestCompaction();

        } catch (IOException e) {
            System.err.println("Compaction execution failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isPaused() {
        return paused.get();
    }

    public CompactionStrategy getStrategy() {
        return strategy;
    }
}