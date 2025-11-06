package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

/**
 * Block读取器，负责读取和解析数据块
 */
public class Block {
    private final byte[] data;
    private final int numRestarts;
    private final Comparator<byte[]> comparator;
    private final int restartsOffset;

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

        // 计算重启点区域的起始位置
        this.restartsOffset = data.length - 4 - numRestarts * 4;
        if (restartsOffset < 0) {
            throw new IllegalArgumentException("Invalid block structure");
        }
    }

    /**
     * 创建Block迭代器
     */
    public BlockIterator iterator() {
        return new BlockIterator(data, numRestarts, restartsOffset, comparator);
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

    public byte[] getData() {
        return data;
    }

    /**
     * Block迭代器
     */
    public static class BlockIterator {
        private final byte[] data;
        private final int numRestarts;
        private final int restartsOffset;
        private final Comparator<byte[]> comparator;
        private final int[] restartPositions;

        private int currentEntry;
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean valid;

        private BlockIterator(byte[] data, int numRestarts, int restartsOffset, Comparator<byte[]> comparator) {
            this.data = data;
            this.numRestarts = numRestarts;
            this.restartsOffset = restartsOffset;
            this.comparator = comparator;
            this.currentEntry = -1;
            this.valid = false;
            this.restartPositions = loadRestartPositions();
        }

        public boolean isValid() {
            return valid;
        }

        public void seekToFirst() {
            if (getEntryCount() == 0) {
                valid = false;
                return;
            }
            currentEntry = 0;
            parseEntry(0);
            valid = currentEntry < getEntryCount();
        }

        public void seek(byte[] target) {
            if (target == null) {
                seekToFirst();
                return;
            }

            // 使用重启点进行二分查找优化
            int leftRestart = 0;
            int rightRestart = numRestarts - 1;

            // 在重启点间二分查找
            while (leftRestart <= rightRestart) {
                int midRestart = leftRestart + (rightRestart - leftRestart) / 2;
                int entryIndex = getRestartEntryIndex(midRestart);
                parseEntry(entryIndex);

                // 确保 parseEntry 成功
                if (!isValid()) {
                    // 如果解析失败（例如，entryIndex无效），则停止二分查找
                    break;
                }

                int cmp = comparator.compare(currentKey, target);
                if (cmp < 0) {
                    leftRestart = midRestart + 1;
                } else if (cmp > 0) {
                    rightRestart = midRestart - 1;
                } else {
                    // === 修复点 1：更新 currentEntry ===
                    // 找到了完全匹配
                    currentEntry = entryIndex;
                    valid = true;
                    return;
                }
            }

            // 在找到的重启点段内线性搜索
            // leftRestart 现在是第一个 >= target 的重启点索引

            // segmentStart 是上一个重启点的条目索引
            int segmentStart = (leftRestart > 0) ? getRestartEntryIndex(leftRestart - 1) : 0;

            // 我们应该从 segmentStart 扫描到块的末尾（由 getEntryCount() 估算）。
            int segmentEnd = getEntryCount();

            for (int i = segmentStart; i < segmentEnd; i++) {
                parseEntry(i);

                // parseEntry 可能会因为索引超出实际范围而设置 valid = false
                if (!isValid()) {
                    break;
                }

                int cmp = comparator.compare(currentKey, target);
                if (cmp >= 0) {
                    // 找到第一个 >= target 的键
                    currentEntry = i;
                    valid = true;
                    return;
                }
            }

            // 没找到，定位到文件末尾
            currentEntry = getEntryCount();
            valid = false;
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
            return currentKey.clone();
        }

        public byte[] value() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentValue.clone();
        }

        /**
         * 通过实际解析数据区域来计算准确的条目数量
         */
        private int getEntryCount() {
            if (restartsOffset == 0) return 0;

            int count = 0;
            int currentPos = 0;

            try {
                ByteBuffer buffer = ByteBuffer.wrap(data);

                while (currentPos < restartsOffset) {
                    buffer.position(currentPos);

                    // 读取记录头
                    int shared = buffer.getInt();
                    int nonShared = buffer.getInt();
                    int valueLen = buffer.getInt();

                    // 验证数据有效性
                    if (shared < 0 || nonShared < 0 || valueLen < 0) {
                        break; // 无效数据
                    }

                    int entrySize = 12 + nonShared + valueLen; // 头部12字节 + 非共享键 + 值
                    if (currentPos + entrySize > restartsOffset) {
                        break; // 超出数据区域
                    }

                    count++;
                    currentPos += entrySize;
                }
            } catch (Exception e) {
                // 解析过程中出现异常，返回当前已计算的计数
            }

            return count;
        }

        /**
         * 获取重启点对应的条目索引
         */
        private int getRestartEntryIndex(int restartIndex) {
            if (restartIndex < 0 || restartIndex >= numRestarts) {
                throw new IllegalArgumentException("Invalid restart index: " + restartIndex);
            }
            return restartPositions[restartIndex];
        }

        /**
         * 加载重启点位置数组
         */
        private int[] loadRestartPositions() {
            int[] positions = new int[numRestarts];
            ByteBuffer buffer = ByteBuffer.wrap(data);
            buffer.position(restartsOffset);

            for (int i = 0; i < numRestarts; i++) {
                positions[i] = buffer.getInt();
            }
            return positions;
        }

        /**
         * 解析指定索引的条目
         */
        private void parseEntry(int entryIndex) {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                buffer.position(0);

                byte[] prevKey = new byte[0];
                int currentPos = 0;
                int currentIndex = 0;

                while (currentPos < restartsOffset && currentIndex <= entryIndex) {
                    buffer.position(currentPos);

                    // 读取前缀压缩格式
                    int shared = buffer.getInt();
                    int nonShared = buffer.getInt();
                    int valueLen = buffer.getInt();

                    // 检查边界
                    if (shared < 0 || nonShared < 0 || valueLen < 0 ||
                            currentPos + 12 + nonShared + valueLen > restartsOffset) {
                        valid = false;
                        return;
                    }

                    // 读取非共享键部分和值
                    byte[] nonSharedKey = new byte[nonShared];
                    byte[] value = new byte[valueLen];

                    if (nonShared > 0) {
                        buffer.get(nonSharedKey);
                    }
                    if (valueLen > 0) {
                        buffer.get(value);
                    }

                    // 重建完整键
                    byte[] key = new byte[shared + nonShared];
                    if (shared > 0) {
                        System.arraycopy(prevKey, 0, key, 0, Math.min(shared, prevKey.length));
                    }
                    if (nonShared > 0) {
                        System.arraycopy(nonSharedKey, 0, key, shared, nonShared);
                    }

                    // 更新位置
                    currentPos = buffer.position();

                    // 如果是目标条目
                    if (currentIndex == entryIndex) {
                        currentKey = key;
                        currentValue = value;
                        currentEntry = entryIndex;
                        valid = true;
                        return;
                    }

                    prevKey = key;
                    currentIndex++;
                }

                // 条目索引超出范围
                valid = false;

            } catch (Exception e) {
                valid = false;
            }
        }

        /**
         * 获取当前条目索引（用于调试）
         */
        public int getCurrentEntry() {
            return currentEntry;
        }
    }

    /**
     * 获取重启点数量（用于测试）
     */
    public int getNumRestarts() {
        return numRestarts;
    }

    /**
     * 获取数据区域大小（用于测试）
     */
    public int getDataSize() {
        return restartsOffset;
    }
}