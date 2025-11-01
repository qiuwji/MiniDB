package com.qiu.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 写入批处理
 */
public class WriteBatch {

    private final List<WriteOp> operations;

    // [NEW] 新增字段，用于存储此批处理的起始序列号
    // -1 表示尚未分配
    private long sequenceNumber = -1;

    public WriteBatch() {
        this.operations = new ArrayList<>();
    }

    /**
     * 添加一个 'Put' 操作
     */
    public void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");
        operations.add(new WriteOp(false, key, value));
    }

    /**
     * 添加一个 'Delete' 操作
     */
    public void delete(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");
        operations.add(new WriteOp(true, key, null));
    }

    /**
     * 获取此批处理中的所有操作
     */
    public List<WriteOp> getOperations() {
        return operations;
    }

    /**
     * [NEW] 设置此批处理的起始序列号
     */
    public void setSequenceNumber(long seq) {
        this.sequenceNumber = seq;
    }

    /**
     * [NEW] 获取此批处理的起始序列号
     */
    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * 获取此批处理中的操作数量
     */
    public int size() {
        return operations.size();
    }

    /**
     * 检查此批处理是否为空
     */
    public boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * 清空此批处理
     */
    public void clear() {
        operations.clear();
        this.sequenceNumber = -1;
    }

    /**
     * 内部类，代表一个单独的写操作
     */
    public static class WriteOp {
        public final boolean isDelete;
        public final byte[] key;
        public final byte[] value;

        WriteOp(boolean isDelete, byte[] key, byte[] value) {
            this.isDelete = isDelete;
            this.key = key;
            this.value = value;
        }
    }
}