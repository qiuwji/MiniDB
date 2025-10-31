package com.qiu.sstable;

/**
 * SSTable文件尾部，包含元数据索引和数据的索引
 */
public class Footer {
    public static final int ENCODED_LENGTH = 48; // 2 * BlockHandle最大编码长度 + 魔数
    public static final long MAGIC_NUMBER = 0xdb4775248b80fb57L; // 随机魔数

    private final BlockHandle metaIndexHandle;
    private final BlockHandle indexHandle;

    public Footer(BlockHandle metaIndexHandle, BlockHandle indexHandle) {
        this.metaIndexHandle = metaIndexHandle;
        this.indexHandle = indexHandle;
    }

    public BlockHandle getMetaIndexHandle() {
        return metaIndexHandle;
    }

    public BlockHandle getIndexHandle() {
        return indexHandle;
    }

    @Override
    public String toString() {
        return "Footer{metaIndex=" + metaIndexHandle + ", index=" + indexHandle + "}";
    }
}
