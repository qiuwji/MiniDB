package com.qiu.core;

import java.util.ArrayList;
import java.util.List;

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

    public void put(byte[] key, byte[] value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        if (value == null) throw new IllegalArgumentException("Value cannot be null");
        operations.add(new WriteOp(key.clone(), value.clone(), false));
    }

    public void put(Slice key, Slice value) {
        put(key.getData(), value.getData());
    }

    public void delete(byte[] key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        operations.add(new WriteOp(key.clone(), null, true));
    }

    public void delete(Slice key) {
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
     * 获取操作数据的近似大小
     */
    public int approximateSize() {
        int size = 0;
        for (WriteOp op : operations) {
            size += op.key.length;
            if (op.value != null) {
                size += op.value.length;
            }
        }
        return size;
    }
}
