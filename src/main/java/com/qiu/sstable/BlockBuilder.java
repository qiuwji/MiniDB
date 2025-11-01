package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Block构建器，负责构建数据块
 */
public class BlockBuilder {
    private final List<byte[]> keys;
    private final List<byte[]> values;
    private final int blockSize;
    private final Comparator<byte[]> comparator;
    private int currentSize;
    private byte[] lastKey;

    public BlockBuilder(int blockSize) {
        this(blockSize, new BytewiseComparator());
    }

    public BlockBuilder(int blockSize, Comparator<byte[]> comparator) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("Block size must be positive");
        }
        this.blockSize = blockSize;
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.currentSize = 0;
        this.lastKey = null;
    }

    /**
     * 添加键值对到Block
     */
    public void add(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // 检查键的顺序（必须升序）
        if (lastKey != null && comparator.compare(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be in ascending order");
        }

        int entrySize = key.length + value.length + 12; // 3 * 4字节长度前缀（shared, nonShared, valueLen）
        if (currentSize + entrySize > blockSize && !isEmpty()) {
            throw new IllegalStateException("Block size exceeded");
        }

        keys.add(key);
        values.add(value);
        currentSize += entrySize;
        lastKey = key;
    }

    /**
     * 完成Block构建，返回序列化数据
     */
    public byte[] finish() {
        if (isEmpty()) {
            return new byte[0];
        }

        // 计算所需缓冲区大小
        int keyValueSize = 0;
        for (int i = 0; i < keys.size(); i++) {
            // 每条记录写入： shared(int=4) + nonShared(int=4) + valueLen(int=4) + nonSharedBytes + valueBytes
            // 使用 key.length 作为 nonShared 的上界（安全估计）
            keyValueSize += 12 + keys.get(i).length + values.get(i).length;
        }

        // 重启点数组大小： numRestarts * 4 (每个重启点的偏移) + 4 (重启点数量)
        int restartInterval = 16;
        int numRestarts = (keys.size() + restartInterval - 1) / restartInterval;
        int restartsSize = numRestarts * 4 + 4;

        int totalSize = keyValueSize + restartsSize;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // 写入键值对
        byte[] prevKey = new byte[0];
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            byte[] value = values.get(i);

            // 计算共享前缀长度
            int shared = 0;
            while (shared < prevKey.length && shared < key.length &&
                    prevKey[shared] == key[shared]) {
                shared++;
            }

            int nonShared = key.length - shared;

            // 写入共享长度、非共享长度、值长度（3 个 int）
            buffer.putInt(shared);
            buffer.putInt(nonShared);
            buffer.putInt(value.length);

            // 写入非共享键部分和值
            if (nonShared > 0) {
                buffer.put(key, shared, nonShared);
            }
            if (value.length > 0) {
                buffer.put(value);
            }

            prevKey = key;
        }

        // 写入重启点（这里我们将重启点写为条目索引位置）
        for (int i = 0; i < keys.size(); i += restartInterval) {
            buffer.putInt(i);
        }

        // 写入重启点数量
        buffer.putInt(numRestarts);

        return buffer.array();
    }

    /**
     * 重置构建器状态
     */
    public void reset() {
        keys.clear();
        values.clear();
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
     * 获取剩余空间
     */
    public int remainingCapacity() {
        return blockSize - currentSize;
    }
}
