package com.qiu.memory;

import com.qiu.core.WriteBatch;
import com.qiu.util.BytewiseComparator;

import java.util.*;
import java.util.Objects;

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
     * 查找键对应的值（使用 InternalKey）
     */
    public byte[] get(InternalKey key) {
        Objects.requireNonNull(key, "Key cannot be null");

        // 由于跳表中相同userKey但不同sequence的键是排序的，我们只需要找到第一个匹配的
        // 因为序列号是降序排列，第一个匹配的就是最新的版本
        Iterator<SkipList.Entry<InternalKey, byte[]>> iterator = table.iterator();
        while (iterator.hasNext()) {
            SkipList.Entry<InternalKey, byte[]> entry = iterator.next();
            InternalKey currentKey = entry.getKey();

            // 比较用户键（忽略序列号）
            if (compareUserKeys(currentKey.getUserKey(), key.getUserKey()) == 0) {
                byte[] value = entry.getValue();

                // 如果是删除标记，返回null
                if (currentKey.isDeletion()) {
                    return null;
                }

                // 返回值的防御性拷贝
                return value != null ? value.clone() : null;
            }

            // 由于键是排序的，如果当前用户键大于目标键，可以提前结束
            if (compareUserKeys(currentKey.getUserKey(), key.getUserKey()) > 0) {
                break;
            }
        }

        return null;
    }

    /**
     * 查找用户键对应的值（用于数据库查询）
     */
    public byte[] get(byte[] userKey) {
        Objects.requireNonNull(userKey, "User key cannot be null");

        // 创建一个虚拟的InternalKey用于查找（使用最大序列号确保找到最新版本）
        InternalKey lookupKey = new InternalKey(userKey, Long.MAX_VALUE, InternalKey.ValueType.VALUE);

        Iterator<SkipList.Entry<InternalKey, byte[]>> iterator = table.iterator();
        while (iterator.hasNext()) {
            SkipList.Entry<InternalKey, byte[]> entry = iterator.next();
            InternalKey currentKey = entry.getKey();

            // 比较用户键
            int cmp = compareUserKeys(currentKey.getUserKey(), userKey);
            if (cmp == 0) {
                // 找到匹配的用户键
                byte[] value = entry.getValue();

                if (currentKey.isDeletion()) {
                    return null; // 墓碑标记
                }

                return value != null ? value.clone() : null; // 防御性拷贝
            }

            if (cmp > 0) {
                break; // 已超过可能的范围
            }
        }

        return null; // 未找到
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
            while (iter.isValid()) {
                entries.add(new SkipList.Entry<>(iter.key(), iter.value()));
                iter.next();
            }
            return entries;
        }

        @Override
        public String toString() {
            return String.format("MemTableIterator{valid=%s, key=%s}",
                    valid, valid ? key().getUserKey() : "null");
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
        sb.append("]}");
        return sb.toString();
    }
}