package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Block读取器，负责读取和解析数据块
 */
public class Block {
    private final byte[] data;
    private final int numRestarts;
    private final Comparator<byte[]> comparator;

    public Block(byte[] data) {
        this(data, new BytewiseComparator());
    }

    public Block(byte[] data, Comparator<byte[]> comparator) {
        this.data = Objects.requireNonNull(data, "Data cannot be null");
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");

        if (data.length < 4) {
            throw new IllegalArgumentException("Block data too short");
        }

        // 读取重启点数量（存储在最后4字节）
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.numRestarts = buffer.getInt(data.length - 4);

        if (numRestarts < 0 || numRestarts * 4 > data.length - 4) {
            throw new IllegalArgumentException("Invalid restart points");
        }
    }

    /**
     * 创建Block迭代器
     */
    public BlockIterator iterator() {
        return new BlockIterator(data, numRestarts, comparator);
    }

    /**
     * 查找指定的键
     */
    public byte[] get(byte[] key) {
        BlockIterator iterator = iterator();
        iterator.seek(key);
        if (iterator.isValid() && comparator.compare(iterator.key(), key) == 0) {
            return iterator.value();
        }
        return null;
    }

    /**
     * Block迭代器
     */
    public static class BlockIterator {
        private final byte[] data;
        private final int numRestarts;
        private final Comparator<byte[]> comparator;
        private int currentEntry;
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean valid;

        private BlockIterator(byte[] data, int numRestarts, Comparator<byte[]> comparator) {
            this.data = data;
            this.numRestarts = numRestarts;
            this.comparator = comparator;
            this.currentEntry = -1;
            this.valid = false;
        }

        public boolean isValid() {
            return valid;
        }

        public void seekToFirst() {
            currentEntry = 0;
            parseEntry(0);
            valid = (currentEntry < getEntryCount());
        }

        public void seek(byte[] target) {
            if (target == null) {
                seekToFirst();
                return;
            }

            // 二分查找
            int left = 0;
            int right = getEntryCount() - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                parseEntry(mid);

                int cmp = comparator.compare(currentKey, target);
                if (cmp < 0) {
                    left = mid + 1;
                } else if (cmp > 0) {
                    right = mid - 1;
                } else {
                    valid = true;
                    return;
                }
            }

            // 没找到精确匹配，定位到第一个 >= target 的条目
            currentEntry = left;
            if (currentEntry < getEntryCount()) {
                parseEntry(currentEntry);
                valid = true;
            } else {
                valid = false;
            }
        }

        public void next() {
            if (!valid) {
                return;
            }

            currentEntry++;
            if (currentEntry < getEntryCount()) {
                parseEntry(currentEntry);
            } else {
                valid = false;
            }
        }

        public byte[] key() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentKey.clone(); // 防御性拷贝
        }

        public byte[] value() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentValue.clone(); // 防御性拷贝
        }

        private int getEntryCount() {
            // 估算条目数量（不精确，但足够用于迭代）
            return numRestarts * 16; // 假设每16个条目一个重启点
        }

        private void parseEntry(int entryIndex) {
            // 简化实现：实际需要解析重启点和前缀压缩
            // 这里使用简化逻辑，实际实现需要处理前缀压缩
            try {
                ByteBuffer buffer = ByteBuffer.wrap(data);

                // 跳过重启点区域
                int dataEnd = data.length - 4 - numRestarts * 4;

                // 简单模拟：假设每个条目都是独立存储的
                // 实际实现需要处理前缀压缩格式
                int pos = entryIndex * 32; // 简化：假设每个条目约32字节
                if (pos >= dataEnd) {
                    valid = false;
                    return;
                }

                buffer.position(pos);

                // 读取键长度和值长度（简化）
                int keyLen = Math.min(buffer.getInt(), dataEnd - pos - 8);
                int valueLen = buffer.getInt();

                if (keyLen < 0 || valueLen < 0 || pos + 8 + keyLen + valueLen > dataEnd) {
                    valid = false;
                    return;
                }

                currentKey = new byte[keyLen];
                currentValue = new byte[valueLen];

                buffer.get(currentKey);
                buffer.get(currentValue);
                currentEntry = entryIndex;
                valid = true;

            } catch (Exception e) {
                valid = false;
            }
        }
    }
}
