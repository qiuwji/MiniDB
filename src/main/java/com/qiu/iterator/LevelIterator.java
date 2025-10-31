// src/main/java/com/github/minidb/iterator/LevelIterator.java
package com.qiu.iterator;

import com.qiu.sstable.SSTable;
import com.qiu.version.FileMetaData;
import com.qiu.version.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 层级迭代器，迭代一个层级中的所有SSTable文件
 */
public class LevelIterator implements SeekingIterator<byte[], byte[]> {
    private final Version version;
    private final int level;
    private final List<FileMetaData> files;
    private final List<SeekingIterator<byte[], byte[]>> iterators;
    private final MergeIterator<byte[], byte[]> mergeIterator;
    private int currentIteratorIndex;
    private boolean valid;
    private int blocksRead;

    public LevelIterator(Version version, int level) throws IOException {
        this.version = Objects.requireNonNull(version, "Version cannot be null");
        this.level = level;
        this.files = version.getFiles(level);
        this.iterators = new ArrayList<>();
        this.blocksRead = 0;
        this.valid = false;

        // 为层级中的每个文件创建迭代器
        initializeIterators();

        // 创建合并迭代器
        this.mergeIterator = new MergeIterator<>(iterators, new com.qiu.util.BytewiseComparator());
        this.valid = mergeIterator.isValid();
    }

    /**
     * 初始化文件迭代器
     */
    private void initializeIterators() throws IOException {
        for (FileMetaData file : files) {
            try {
                String tablePath = version.getVersionSet().getTableFileName(file.getFileNumber());
                SSTable table = new SSTable(tablePath);
                SeekingIterator<byte[], byte[]> iterator = new TableIterator(table.iterator());
                iterators.add(iterator);
            } catch (IOException e) {
                System.err.println("Failed to open SSTable: " + file.getFileNumber());
                throw e;
            }
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
        blocksRead++; // 粗略估计块读取
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
     * 检查该层级是否有数据
     */
    public boolean hasData() {
        return !files.isEmpty();
    }

    /**
     * 获取层级号
     */
    public int getLevel() {
        return level;
    }

    /**
     * 获取文件数量
     */
    public int getFileCount() {
        return files.size();
    }

    /**
     * 获取读取的块数量
     */
    public int getBlocksRead() {
        return blocksRead;
    }

    @Override
    public String toString() {
        return String.format("LevelIterator{L%d, files=%d, valid=%s}",
                level, files.size(), valid);
    }
}