package com.qiu.core;

import com.qiu.wal.LogConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 批量写操作
 */
public class WriteBatch {
    public static class WriteOp {
        public final byte[] key;
        public final byte[] value;
        public final boolean isDelete;

        WriteOp(byte[] key, byte[] value, boolean isDelete) {
            this.key = key;
            this.value = value;
            this.isDelete = isDelete;
        }
    }

    private final List<WriteOp> operations = new ArrayList<>();

    public WriteBatch() {
        // 默认构造函数
    }

    /**
     * 复制构造函数
     */
    public WriteBatch(WriteBatch other) {
        for (WriteOp op : other.operations) {
            this.operations.add(new WriteOp(
                    op.key.clone(),
                    op.value != null ? op.value.clone() : null,
                    op.isDelete
            ));
        }
    }

    public void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");
        if (key.length == 0) throw new IllegalArgumentException("Key cannot be empty");

        // 检查大小限制
        checkSizeLimitBeforeAdd(key, value, false);

        operations.add(new WriteOp(key.clone(), value.clone(), false));
    }

    public void put(Slice key, Slice value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");
        put(key.getData(), value.getData());
    }

    public void delete(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (key.length == 0) throw new IllegalArgumentException("Key cannot be empty");

        // 检查大小限制
        checkSizeLimitBeforeAdd(key, null, true);

        operations.add(new WriteOp(key.clone(), null, true));
    }

    public void delete(Slice key) {
        Objects.requireNonNull(key, "Key cannot be null");
        delete(key.getData());
    }

    public void clear() {
        operations.clear();
    }

    public int size() {
        return operations.size();
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * 获取所有操作（仅供内部使用）
     */
    public List<WriteOp> getOperations() {
        return new ArrayList<>(operations);
    }

    /**
     * 获取操作数据的近似大小（用于内存估算）
     */
    public int approximateSize() {
        int size = 0;
        for (WriteOp op : operations) {
            size += op.key.length;
            if (op.value != null) {
                size += op.value.length;
            }
            // 添加操作元数据的估算大小
            size += 16; // 引用、标志位等的估算
        }
        return size;
    }

    /**
     * 计算序列化后的大小（用于WAL写入检查）
     */
    public int calculateSerializedSize() {
        int size = 8; // 序列号（8字节）
        for (WriteOp op : operations) {
            size += 1; // 操作类型（1字节）
            size += 4 + op.key.length; // 键长度（4字节）+ 键数据

            if (!op.isDelete) {
                if (op.value != null) {
                    size += 4 + op.value.length; // 值长度（4字节）+ 值数据
                } else {
                    size += 4; // 空值的长度字段
                }
            }
        }
        return size;
    }

    /**
     * 检查序列化大小是否超过限制
     */
    public void checkSizeLimit() {
        int size = calculateSerializedSize();
        if (size > LogConstants.MAX_RECORD_SIZE) {
            throw new IllegalStateException(
                    "WriteBatch too large: " + size + " bytes (max: " +
                            LogConstants.MAX_RECORD_SIZE + "). " +
                            "Contains " + operations.size() + " operations.");
        }
    }

    /**
     * 在添加操作前检查大小限制
     */
    private void checkSizeLimitBeforeAdd(byte[] key, byte[] value, boolean isDelete) {
        // 估算新操作增加的大小
        int newOpSize = 1 + 4 + key.length; // 类型 + 键长 + 键数据
        if (!isDelete && value != null) {
            newOpSize += 4 + value.length; // 值长 + 值数据
        }

        int currentSize = calculateSerializedSize();
        int totalSize = currentSize + newOpSize;

        if (totalSize > LogConstants.MAX_RECORD_SIZE) {
            throw new IllegalStateException(
                    "Adding operation would exceed WriteBatch size limit: " +
                            totalSize + " bytes (max: " + LogConstants.MAX_RECORD_SIZE + "). " +
                            "Current batch has " + operations.size() + " operations.");
        }
    }

    /**
     * 安全地添加操作，如果超过大小限制则返回false
     */
    public boolean safePut(byte[] key, byte[] value) {
        try {
            put(key, value);
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    /**
     * 安全地删除操作，如果超过大小限制则返回false
     */
    public boolean safeDelete(byte[] key) {
        try {
            delete(key);
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    /**
     * 获取批次的统计信息
     */
    public BatchStats getStats() {
        int putCount = 0;
        int deleteCount = 0;
        int totalKeySize = 0;
        int totalValueSize = 0;

        for (WriteOp op : operations) {
            if (op.isDelete) {
                deleteCount++;
            } else {
                putCount++;
            }
            totalKeySize += op.key.length;
            if (op.value != null) {
                totalValueSize += op.value.length;
            }
        }

        return new BatchStats(putCount, deleteCount, totalKeySize, totalValueSize);
    }

    /**
     * 批量操作统计信息
     */
    public static class BatchStats {
        public final int putCount;
        public final int deleteCount;
        public final int totalKeySize;
        public final int totalValueSize;

        public BatchStats(int putCount, int deleteCount, int totalKeySize, int totalValueSize) {
            this.putCount = putCount;
            this.deleteCount = deleteCount;
            this.totalKeySize = totalKeySize;
            this.totalValueSize = totalValueSize;
        }

        @Override
        public String toString() {
            return String.format(
                    "BatchStats{puts=%d, deletes=%d, keySize=%d, valueSize=%d}",
                    putCount, deleteCount, totalKeySize, totalValueSize
            );
        }
    }

    @Override
    public String toString() {
        BatchStats stats = getStats();
        return String.format(
                "WriteBatch{size=%d, serializedSize=%d, %s}",
                operations.size(), calculateSerializedSize(), stats
        );
    }

    /**
     * 创建当前批次的副本
     */
    public WriteBatch copy() {
        return new WriteBatch(this);
    }

    /**
     * 合并另一个批次（不检查大小限制，调用者需确保）
     */
    public void merge(WriteBatch other) {
        for (WriteOp op : other.operations) {
            this.operations.add(new WriteOp(
                    op.key.clone(),
                    op.value != null ? op.value.clone() : null,
                    op.isDelete
            ));
        }
    }

    /**
     * 检查是否包含指定的键（用于测试和调试）
     */
    public boolean containsKey(byte[] key) {
        for (WriteOp op : operations) {
            if (java.util.Arrays.equals(op.key, key)) {
                return true;
            }
        }
        return false;
    }
}