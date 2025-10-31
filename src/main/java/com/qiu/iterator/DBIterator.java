package com.qiu.iterator;

import com.qiu.memory.MemTable;
import com.qiu.sstable.SSTable;
import com.qiu.version.Version;
import com.qiu.version.VersionSet;

import java.io.IOException;
import java.util.*;

/**
 * 数据库迭代器，合并内存表和SSTable的数据
 */
public class DBIterator implements SeekingIterator<byte[], byte[]> {
    private final Version version;
    private final MemTable memTable;
    private final MemTable immutableMemTable;
    private final List<LevelIterator> levelIterators;
    private final Comparator<byte[]> comparator;

    // 使用合并迭代器来统一处理所有数据源
    private final List<SeekingIterator<byte[], byte[]>> allIterators;
    private final MergeIterator<byte[], byte[]> mergeIterator;

    private boolean valid;

    public DBIterator(Version version, MemTable memTable, MemTable immutableMemTable) {
        this.version = Objects.requireNonNull(version, "Version cannot be null");
        this.memTable = memTable;
        this.immutableMemTable = immutableMemTable;
        this.comparator = new com.qiu.util.BytewiseComparator();
        this.levelIterators = new ArrayList<>();
        this.allIterators = new ArrayList<>();

        initializeIterators();

        // 创建合并迭代器
        this.mergeIterator = new MergeIterator<>(allIterators, comparator);
        this.valid = mergeIterator.isValid();
    }

    /**
     * 初始化所有层级的迭代器
     */
    private void initializeIterators() {
        try {
            // 为每个SSTable文件创建迭代器
            for (int level = 0; level < version.getLevelCount(); level++) {
                LevelIterator levelIterator = new LevelIterator(version, level);
                if (levelIterator.hasData()) {
                    levelIterators.add(levelIterator);
                    allIterators.add(levelIterator);
                }
            }

            // 添加内存表迭代器
            if (memTable != null && !memTable.isEmpty()) {
                allIterators.add(new MemTableIteratorAdapter(memTable));
            }

            // 添加不可变内存表迭代器
            if (immutableMemTable != null && !immutableMemTable.isEmpty()) {
                allIterators.add(new MemTableIteratorAdapter(immutableMemTable));
            }

        } catch (IOException e) {
            System.err.println("Failed to initialize iterators: " + e.getMessage());
        }
    }

    @Override
    public boolean isValid() {
        return valid && mergeIterator.isValid();
    }

    @Override
    public void seekToFirst() throws IOException {
        mergeIterator.seekToFirst();
        valid = mergeIterator.isValid();
    }

    @Override
    public void seek(byte[] key) throws IOException {
        Objects.requireNonNull(key, "Key cannot be null");
        mergeIterator.seek(key);
        valid = mergeIterator.isValid();
    }

    @Override
    public void next() throws IOException {
        if (!valid) {
            throw new NoSuchElementException("Iterator is not valid");
        }

        mergeIterator.next();
        valid = mergeIterator.isValid();
    }

    @Override
    public byte[] key() {
        if (!valid) {
            throw new IllegalStateException("Iterator is not valid");
        }
        return mergeIterator.key();
    }

    @Override
    public byte[] value() {
        if (!valid) {
            throw new IllegalStateException("Iterator is not valid");
        }
        return mergeIterator.value();
    }

    @Override
    public void close() throws IOException {
        mergeIterator.close();
        valid = false;
    }

    /**
     * 内存表迭代器适配器
     */
    private class MemTableIteratorAdapter implements SeekingIterator<byte[], byte[]> {
        private final MemTable memTable;
        private MemTable.MemTableIterator iterator;
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean valid;

        public MemTableIteratorAdapter(MemTable memTable) {
            this.memTable = memTable;
            this.iterator = memTable.iterator();
            this.valid = iterator.isValid();
            updateCurrent();
        }

        @Override
        public boolean isValid() {
            return valid;
        }

        @Override
        public void seekToFirst() {
            iterator.seekToFirst();
            valid = iterator.isValid();
            updateCurrent();
        }

        @Override
        public void seek(byte[] key) {
            // 内存表迭代器不支持随机seek，只能从头开始遍历
            seekToFirst();
            while (valid && comparator.compare(currentKey, key) < 0) {
                next();
            }
        }

        @Override
        public void next() {
            if (!valid) {
                throw new NoSuchElementException("Iterator is not valid");
            }
            iterator.next();
            valid = iterator.isValid();
            updateCurrent();
        }

        @Override
        public byte[] key() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentKey;
        }

        @Override
        public byte[] value() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentValue;
        }

        @Override
        public void close() {
            // 内存表迭代器没有资源需要释放
            valid = false;
            currentKey = null;
            currentValue = null;
        }

        private void updateCurrent() {
            if (valid) {
                currentKey = iterator.key().getUserKey();
                currentValue = iterator.value();
                // 跳过删除标记
                if (currentValue == null) {
                    next(); // 递归跳过删除标记
                }
            } else {
                currentKey = null;
                currentValue = null;
            }
        }
    }

    /**
     * 获取迭代器统计信息
     */
    public IteratorStats getStats() {
        int sstablesAccessed = 0;
        int blocksRead = 0;

        for (LevelIterator iterator : levelIterators) {
            if (iterator.hasData()) {
                sstablesAccessed++;
                blocksRead += iterator.getBlocksRead();
            }
        }

        return new IteratorStats(sstablesAccessed, blocksRead);
    }

    @Override
    public String toString() {
        return String.format("DBIterator{valid=%s, iterators=%d}",
                valid, allIterators.size());
    }
}