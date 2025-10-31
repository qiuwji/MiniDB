package com.qiu.sstable;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * SSTable迭代器
 */
public class TableIterator {
    private final SSTable table;
    private Block.BlockIterator currentBlockIter;
    private Block.BlockIterator indexIter;
    private boolean valid;

    public TableIterator(SSTable table) throws IOException {
        this.table = table;
        this.indexIter = table.indexBlock.iterator();
        this.valid = false;

        // 定位到第一个数据块
        if (indexIter.isValid()) {
            loadBlock(indexIter.value());
            if (currentBlockIter != null) {
                currentBlockIter.seekToFirst();
                valid = currentBlockIter.isValid();
            }
        }
    }

    public boolean isValid() {
        return valid;
    }

    public void seekToFirst() throws IOException {
        indexIter.seekToFirst();
        if (indexIter.isValid()) {
            loadBlock(indexIter.value());
            if (currentBlockIter != null) {
                currentBlockIter.seekToFirst();
                valid = currentBlockIter.isValid();
            } else {
                valid = false;
            }
        } else {
            valid = false;
        }
    }

    public void seek(byte[] target) throws IOException {
        if (target == null) {
            seekToFirst();
            return;
        }

        // 在索引块中查找
        indexIter.seek(target);
        if (!indexIter.isValid()) {
            valid = false;
            return;
        }

        // 加载数据块
        loadBlock(indexIter.value());
        if (currentBlockIter != null) {
            currentBlockIter.seek(target);
            valid = currentBlockIter.isValid();
        } else {
            valid = false;
        }
    }

    public void next() throws IOException {
        if (!valid) {
            throw new NoSuchElementException();
        }

        currentBlockIter.next();
        if (currentBlockIter.isValid()) {
            return;
        }

        // 当前块已结束，移动到下一个块
        indexIter.next();
        if (!indexIter.isValid()) {
            valid = false;
            return;
        }

        loadBlock(indexIter.value());
        if (currentBlockIter != null) {
            currentBlockIter.seekToFirst();
            valid = currentBlockIter.isValid();
        } else {
            valid = false;
        }
    }

    public byte[] key() {
        if (!valid) {
            throw new IllegalStateException("Iterator is not valid");
        }
        return currentBlockIter.key();
    }

    public byte[] value() {
        if (!valid) {
            throw new IllegalStateException("Iterator is not valid");
        }
        return currentBlockIter.value();
    }

    /**
     * 加载指定块句柄对应的数据块
     */
    private void loadBlock(byte[] handleData) throws IOException {
        BlockHandle handle = table.decodeBlockHandle(handleData);
        Block block = table.readBlock(handle);
        currentBlockIter = block.iterator();
    }
}
