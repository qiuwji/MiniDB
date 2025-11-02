package com.qiu.sstable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Block构建器，负责构建数据块（改进版）
 */
public class BlockBuilder {
    private final List<byte[]> keys;
    private final List<byte[]> values;
    private final int blockSize;
    private final Comparator<byte[]> comparator;
    private final int restartInterval; // 重启点间隔

    private int currentSize;
    private byte[] lastKey; // 上一个添加的键（用于前缀压缩和顺序验证）
    private final List<Integer> restartPositions;  // 重启点位置列表

    public BlockBuilder(int blockSize) {
        this(blockSize, new com.qiu.util.BytewiseComparator(), 16);
    }

    public BlockBuilder(int blockSize, Comparator<byte[]> comparator) {
        this(blockSize, comparator, 16);
    }

    public BlockBuilder(int blockSize, Comparator<byte[]> comparator, int restartInterval) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("Block size must be positive");
        }
        if (restartInterval <= 0) {
            throw new IllegalArgumentException("Restart interval must be positive");
        }

        this.blockSize = blockSize;
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");
        this.restartInterval = restartInterval;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.restartPositions = new ArrayList<>();
        this.currentSize = 0;
        this.lastKey = null;

        // 第一个重启点总是在位置0
        restartPositions.add(0);
    }

    /**
     * 尝试添加键值对，返回是否成功
     */
    public boolean tryAdd(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // 检查键的顺序
        if (lastKey != null && comparator.compare(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be in ascending order. Last key: " +
                    new String(lastKey) + ", current key: " + new String(key));
        }

        // 估算新条目大小
        int estimatedSize = estimateEntrySize(key, value);

        // 检查是否超限（考虑重启点数组的最终大小）
        int estimatedFinalSize = currentSize + estimatedSize + estimateRestartsSize();
        if (estimatedFinalSize > blockSize && !isEmpty()) {
            return false;
        }

        // 检查是否需要添加重启点
        if (keys.size() % restartInterval == 0) {
            restartPositions.add(keys.size());
        }

        // 添加到列表
        keys.add(key);
        values.add(value);
        currentSize += estimatedSize;
        lastKey = key;

        return true;
    }

    /**
     * 强制添加键值对（可能创建超大块）
     */
    public void forceAdd(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // 检查键的顺序
        if (lastKey != null && comparator.compare(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be in ascending order");
        }

        // 检查是否需要添加重启点
        if (keys.size() % restartInterval == 0) {
            restartPositions.add(keys.size());
        }

        keys.add(key);
        values.add(value);

        // 更新大小估算
        currentSize += estimateEntrySize(key, value);
        lastKey = key;
    }

    /**
     * 估算条目大小
     */
    private int estimateEntrySize(byte[] key, byte[] value) {
        // 基础开销：3个int (shared, nonShared, valueLen)
        int baseOverhead = 12;

        // 计算共享前缀长度来估算实际存储的键大小
        int sharedPrefix = 0;
        if (lastKey != null) {
            sharedPrefix = calculateSharedPrefix(lastKey, key);
        }
        int nonShared = key.length - sharedPrefix;

        // 实际存储的数据：非共享键部分 + 值
        int dataSize = nonShared + value.length;

        return baseOverhead + dataSize;
    }

    /**
     * 计算共享前缀长度
     */
    private int calculateSharedPrefix(byte[] prevKey, byte[] currentKey) {
        int minLength = Math.min(prevKey.length, currentKey.length);
        for (int i = 0; i < minLength; i++) {
            if (prevKey[i] != currentKey[i]) {
                return i;
            }
        }
        return minLength;
    }

    /**
     * 估算重启点区域大小
     */
    private int estimateRestartsSize() {
        // 重启点数组：每个重启点4字节 + 重启点数量4字节
        return restartPositions.size() * 4 + 4;
    }

    /**
     * 完成Block构建，返回序列化数据
     */
    public byte[] finish() {
        if (isEmpty()) {
            return new byte[0];
        }

        // 计算精确的缓冲区大小
        int estimatedSize = calculateExactSize();
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        // 写入键值对数据
        byte[] prevKey = new byte[0];
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            byte[] value = values.get(i);

            // 计算共享前缀
            int shared = calculateSharedPrefix(prevKey, key);
            int nonShared = key.length - shared;

            // 写入记录头
            buffer.putInt(shared);
            buffer.putInt(nonShared);
            buffer.putInt(value.length);

            // 写入非共享键部分
            if (nonShared > 0) {
                buffer.put(key, shared, nonShared);
            }

            // 写入值
            if (value.length > 0) {
                buffer.put(value);
            }

            prevKey = key;
        }

        // 写入重启点数组
        for (Integer restartPosition : restartPositions) {
            buffer.putInt(restartPosition);
        }

        // 写入重启点数量
        buffer.putInt(restartPositions.size());

        // 确保缓冲区大小正确
        if (buffer.position() != estimatedSize) {
            // 调整到实际大小
            byte[] result = new byte[buffer.position()];
            System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
            return result;
        }

        return buffer.array();
    }

    /**
     * 计算精确的块大小
     */
    private int calculateExactSize() {
        int size = 0;
        byte[] prevKey = new byte[0];

        // 计算所有记录的大小
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            byte[] value = values.get(i);

            int shared = calculateSharedPrefix(prevKey, key);
            int nonShared = key.length - shared;

            // 记录头 + 非共享键 + 值
            size += 12 + nonShared + value.length;
            prevKey = key;
        }

        // 加上重启点区域
        size += estimateRestartsSize();

        return size;
    }

    /**
     * 重置构建器状态
     */
    public void reset() {
        keys.clear();
        values.clear();
        restartPositions.clear();
        restartPositions.add(0); // 第一个重启点
        currentSize = 0;
        lastKey = null;
    }

    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    /**
     * 获取当前条目数量
     */
    public int entryCount() {
        return keys.size();
    }

    /**
     * 获取当前估算大小
     */
    public int currentSize() {
        return currentSize;
    }

    /**
     * 获取剩余空间估算
     */
    public int remainingCapacity() {
        int estimatedFinalSize = currentSize + estimateRestartsSize();
        return Math.max(0, blockSize - estimatedFinalSize);
    }

    /**
     * 获取块大小
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * 获取当前块的空间使用率
     */
    public double getUsageRatio() {
        int estimatedFinalSize = currentSize + estimateRestartsSize();
        return (double) estimatedFinalSize / blockSize;
    }

    /**
     * 检查是否应该刷新块（基于使用率阈值）
     */
    public boolean shouldFlush() {
        return getUsageRatio() > 0.85; // 85%使用率时建议刷新
    }

    /**
     * 检查是否接近满状态
     */
    public boolean isAlmostFull() {
        return getUsageRatio() > 0.75; // 75%使用率时接近满
    }

    /**
     * 获取重启点数量
     */
    public int getRestartCount() {
        return restartPositions.size();
    }

    /**
     * 获取最后一个键的拷贝
     */
    public byte[] getLastKey() {
        return lastKey != null ? lastKey.clone() : null;
    }

    /**
     * 获取第一个键的拷贝
     */
    public byte[] getFirstKey() {
        return !keys.isEmpty() ? keys.get(0).clone() : null;
    }

    /**
     * 获取键范围（用于索引）
     */
    public KeyRange getKeyRange() {
        if (isEmpty()) {
            return null;
        }
        return new KeyRange(getFirstKey(), getLastKey());
    }

    /**
     * 键范围内部类
     */
    public static class KeyRange {
        private final byte[] firstKey;
        private final byte[] lastKey;

        public KeyRange(byte[] firstKey, byte[] lastKey) {
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        public byte[] getFirstKey() {
            return firstKey;
        }

        public byte[] getLastKey() {
            return lastKey;
        }

        public boolean contains(byte[] key, Comparator<byte[]> comparator) {
            return comparator.compare(key, firstKey) >= 0 &&
                    comparator.compare(key, lastKey) <= 0;
        }
    }

    /**
     * 创建用于超大条目的专用构建器
     */
    public static BlockBuilder createOversizedBuilder(byte[] key, byte[] value) {
        int requiredSize = 12 + key.length + value.length + 8; // 基础 + 重启点
        int blockSize = Math.max(requiredSize * 2, 1024); // 至少1KB或2倍需求
        return new BlockBuilder(blockSize, new com.qiu.util.BytewiseComparator(), 1);
    }

    /**
     * 检查是否是超大条目（单条记录就接近块大小）
     */
    public boolean isOversizedEntry(byte[] key, byte[] value) {
        int entrySize = estimateEntrySize(key, value);
        return entrySize > blockSize * 0.5; // 超过块大小50%就算超大
    }
}