package com.qiu.sstable;

/**
 * Block句柄，表示一个Block在文件中的位置和大小
 */
public record BlockHandle(long offset, long size) {
    public BlockHandle {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative");
        }
    }

    @Override
    public String toString() {
        return "BlockHandle{offset=" + offset + ", size=" + size + "}";
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset) * 31 + Long.hashCode(size);
    }
}
