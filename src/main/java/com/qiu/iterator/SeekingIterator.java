// src/main/java/com/qiu/iterator/SeekingIterator.java
package com.qiu.iterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * 支持定位的迭代器接口
 *
 * 说明：继承 java.io.Closeable 使得所有实现者都可用于 try-with-resources（AutoCloseable）。
 */
public interface SeekingIterator<K, V> extends Closeable {
    /**
     * 检查迭代器是否有效（有当前元素）
     */
    boolean isValid();

    /**
     * 定位到第一个元素
     */
    void seekToFirst() throws IOException;

    /**
     * 定位到指定键（或第一个大于等于该键的元素）
     */
    void seek(K key) throws IOException;

    /**
     * 移动到下一个元素
     */
    void next() throws IOException;

    /**
     * 获取当前键
     */
    K key();

    /**
     * 获取当前值
     */
    V value();

    /**
     * 关闭迭代器，释放资源
     * （来自 Closeable：签名为 close() throws IOException）
     */
    @Override
    void close() throws IOException;
}
