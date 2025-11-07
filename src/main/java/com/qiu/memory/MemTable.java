package com.qiu.memory;

import com.qiu.core.WriteBatch;
import com.qiu.util.BytewiseComparator;

import java.util.*;

/**
 * 内存表，包装跳表并提供墓碑标记支持
 * 内存安全：所有数据访问都进行防御性拷贝
 */
public class MemTable {
    private final SkipList<InternalKey, byte[]> table;
    private long approximateMemoryUsage;
    private final Comparator<byte[]> comparator;

    public MemTable() {
        this.comparator = new BytewiseComparator();
        this.table = new SkipList<>(this::compareInternalKeys);
        this.approximateMemoryUsage = 0;
    }

    /**
     * 比较两个InternalKey
     */
    private int compareInternalKeys(InternalKey a, InternalKey b) {
        return a.compareTo(b);
    }

    /**
     * 插入键值对
     */
    public void put(byte[] key, byte[] value, long sequence, InternalKey.ValueType valueType) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(valueType, "Value type cannot be null");

        InternalKey internalKey = new InternalKey(key, sequence, valueType);
        byte[] storedValue = (valueType == InternalKey.ValueType.VALUE) ?
                Objects.requireNonNull(value, "Value cannot be null for PUT operation").clone() :
                new byte[0]; // 删除操作使用空数组

        table.put(internalKey, storedValue);

        // 更新内存使用估算（key + value + 内部开销）
        approximateMemoryUsage += key.length + storedValue.length + internalKey.encode().length + 16; // 16字节用于内部开销估算
    }

    /**
     * 插入键值对（简化版本）
     */
    public void put(InternalKey key, byte[] value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        table.put(key, value.clone());

        // 更新内存使用估算
        approximateMemoryUsage += key.getUserKey().length + value.length + key.encode().length + 16;
    }

    /**
     * 查找键对应的值（使用 InternalKey，精确匹配）
     */
    public byte[] get(InternalKey key) {
        Objects.requireNonNull(key, "Key cannot be null");

        // 直接使用 SkipList 的精确查找（仅在 caller 使用完整 InternalKey 时适用）
        return table.get(key);
    }

    /**
     * 查找用户键对应的值（用于数据库查询），返回该 userKey 的最新版本（跳过 tombstone）
     */
    public byte[] get(byte[] userKey) {
        Objects.requireNonNull(userKey, "User key cannot be null");

        // 构造查找用的 InternalKey，使用最大序列号确保找到最新版本
        InternalKey lookupKey = new InternalKey(userKey, Long.MAX_VALUE, InternalKey.ValueType.VALUE);

        // 使用 SkipList 的 findGreaterOrEqual 方法高效查找
        SkipList.Entry<InternalKey, byte[]> entry = table.findGreaterOrEqual(lookupKey);
        if (entry == null) {
            return null; // 没有任何 >= 该 key 的条目
        }

        InternalKey foundKey = entry.getKey();

        // 如果 userKey 不相等，说明查找结果已经超过目标范围
        if (compareUserKeys(foundKey.getUserKey(), userKey) != 0) {
            return null;
        }

        // 检查是否是删除标记
        if (foundKey.isDeletion()) {
            return null;
        }

        // 返回防御性拷贝
        byte[] value = entry.getValue();
        return value != null ? value.clone() : null;
    }

    /**
     * 比较两个用户键
     */
    private int compareUserKeys(byte[] a, byte[] b) {
        return comparator.compare(a, b);
    }

    /**
     * 删除键（插入墓碑标记）
     */
    public void delete(byte[] key, long sequence) {
        Objects.requireNonNull(key, "Key cannot be null");
        put(key, new byte[0], sequence, InternalKey.ValueType.DELETION);
    }

    /**
     * 应用批量写入
     */
    public void applyWriteBatch(WriteBatch batch, long sequence) {
        Objects.requireNonNull(batch, "Write batch cannot be null");

        for (WriteBatch.WriteOp op : batch.getOperations()) {
            if (op.isDelete) {
                delete(op.key, sequence);
            } else {
                put(op.key, op.value, sequence, InternalKey.ValueType.VALUE);
            }
        }
    }

    /**
     * 获取近似内存使用量（字节）
     */
    public long approximateSize() {
        return approximateMemoryUsage;
    }

    /**
     * 获取条目数量
     */
    public long size() {
        return table.size();
    }

    /**
     * 是否为空
     */
    public boolean isEmpty() {
        return table.isEmpty();
    }

    /**
     * 清空内存表
     */
    public void clear() {
        table.clear();
        approximateMemoryUsage = 0;
    }

    /**
     * 获取迭代器（按InternalKey顺序）
     */
    public MemTableIterator iterator() {
        return new MemTableIterator(this);
    }

    /**
     * 内存表迭代器 - 完全重写
     */
    public static class MemTableIterator {
        private final MemTable memTable;
        private Iterator<SkipList.Entry<InternalKey, byte[]>> currentIterator;
        private SkipList.Entry<InternalKey, byte[]> currentEntry;
        private boolean valid;

        public MemTableIterator(MemTable memTable) {
            this.memTable = Objects.requireNonNull(memTable, "MemTable cannot be null");
            this.currentIterator = memTable.table.iterator();
            this.valid = false;
            advanceToNextValidEntry();
        }

        /**
         * 检查迭代器是否有效
         */
        public boolean isValid() {
            return valid;
        }

        /**
         * 移动到下一个条目
         */
        public void next() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            advanceToNextValidEntry();
        }

        /**
         * 获取当前键
         */
        public InternalKey key() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentEntry.getKey();
        }

        /**
         * 获取当前值
         */
        public byte[] value() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            byte[] value = currentEntry.getValue();
            return value != null ? value.clone() : null; // 防御性拷贝
        }

        /**
         * 定位到第一个条目
         */
        public void seekToFirst() {
            // 创建新的迭代器来从头开始
            this.currentIterator = memTable.table.iterator();
            this.valid = false;
            this.currentEntry = null;
            advanceToNextValidEntry();
        }

        /**
         * 定位到指定键（近似实现）
         */
        public void seek(byte[] targetKey) {
            // 简化实现：从头开始遍历直到找到目标键或超过目标键
            seekToFirst();
            while (valid) {
                byte[] currentUserKey = currentEntry.getKey().getUserKey();
                int cmp = memTable.compareUserKeys(currentUserKey, targetKey);
                if (cmp >= 0) {
                    break;
                }
                next();
            }
        }

        /**
         * 推进到下一个有效条目（跳过删除标记）
         */
        private void advanceToNextValidEntry() {
            currentEntry = null;
            valid = false;

            while (currentIterator.hasNext()) {
                SkipList.Entry<InternalKey, byte[]> entry = currentIterator.next();

                // 跳过删除标记
                if (!entry.getKey().isDeletion()) {
                    currentEntry = entry;
                    valid = true;
                    break;
                }
            }
        }

        /**
         * 获取所有条目（用于调试）
         */
        public List<SkipList.Entry<InternalKey, byte[]>> getAllEntries() {
            List<SkipList.Entry<InternalKey, byte[]>> entries = new ArrayList<>();
            MemTableIterator iter = memTable.iterator();
            try {
                while (iter.isValid()) {
                    entries.add(new SkipList.Entry<>(iter.key(), iter.value()));
                    iter.next();
                }
            } finally {
                iter.close();
            }
            return entries;
        }

        /**
         * 关闭迭代器，释放底层读锁（如果存在）
         */
        public void close() {
            if (currentIterator instanceof SkipList.SkipListIterator) {
                ((SkipList.SkipListIterator) currentIterator).close();
            }
        }

        @Override
        public String toString() {
            return String.format("MemTableIterator{valid=%s, key=%s}",
                    valid, valid ? Arrays.toString(key().getUserKey()) : "null");
        }

        @Override
        protected void finalize() throws Throwable {
            try {
                close();
            } finally {
                super.finalize();
            }
        }
    }

    /**
     * 获取调试信息
     */
    public String toDebugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MemTable{size=").append(size())
                .append(", memory=").append(approximateSize())
                .append(" bytes, entries=[");

        MemTableIterator iter = iterator();
        boolean first = true;
        try {
            while (iter.isValid()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(new String(iter.key().getUserKey()))
                        .append("=")
                        .append(new String(iter.value()));
                iter.next();
                first = false;
            }
        } finally {
            iter.close();
        }
        sb.append("]}");
        return sb.toString();
    }
}
