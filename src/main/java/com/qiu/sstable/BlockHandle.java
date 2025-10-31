package com.qiu.sstable;

/**
 * Block句柄，表示一个Block在文件中的位置和大小
 */
public class BlockHandle {
    private final long offset;
    private final long size;

    public BlockHandle(long offset, long size) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative");
        }
        this.offset = offset;
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "BlockHandle{offset=" + offset + ", size=" + size + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BlockHandle that = (BlockHandle) obj;
        return offset == that.offset && size == that.size;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset) * 31 + Long.hashCode(size);
    }
}
