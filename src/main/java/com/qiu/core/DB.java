package com.qiu.core;

import com.qiu.iterator.DBIterator;

import java.io.IOException;

/**
 * 数据库核心接口
 */
public interface DB extends AutoCloseable {
    /**
     * 插入或更新键值对
     */
    Status put(byte[] key, byte[] value) throws IOException;

    /**
     * 插入或更新键值对（Slice版本）
     */
    default Status put(Slice key, Slice value) throws IOException {
        return put(key.getData(), value.getData());
    }

    /**
     * 根据键查找值
     */
    byte[] get(byte[] key) throws IOException;

    /**
     * 根据键查找值（Slice版本）
     */
    default byte[] get(Slice key) throws IOException {
        return get(key.getData());
    }

    /**
     * 删除键值对
     */
    Status delete(byte[] key) throws IOException;

    /**
     * 删除键值对（Slice版本）
     */
    default Status delete(Slice key) throws IOException {
        return delete(key.getData());
    }

    /**
     * 写入批量操作
     */
    Status write(WriteBatch batch) throws IOException;

    /**
     * 创建数据库迭代器
     */
    DBIterator iterator() throws IOException;

    /**
     * 获取数据库统计信息
     */
    DBStats getStats();

    /**
     * 关闭数据库
     */
    void close() throws IOException;

    /**
     * 强制数据刷盘
     */
    void flush() throws IOException;

    /**
     * 压缩数据库文件
     */
    void compactRange(byte[] begin, byte[] end) throws IOException;

    /**
     * 获取数据库选项
     */
    Options getOptions();
}