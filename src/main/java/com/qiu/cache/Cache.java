package com.qiu.cache;

/**
 * 缓存接口
 */
public interface Cache {
    /**
     * 将数据插入缓存
     */
    CacheHandle put(String key, byte[] value);

    /**
     * 查找缓存数据
     */
    CacheHandle get(String key);

    /**
     * 删除缓存数据
     */
    boolean delete(String key);

    /**
     * 清空缓存
     */
    void clear();

    /**
     * 获取缓存当前大小（字节）
     */
    long getSize();

    /**
     * 获取缓存容量（字节）
     */
    long getCapacity();

    /**
     * 获取缓存条目数量
     */
    int getEntryCount();

    /**
     * 释放缓存句柄（内部使用）
     */
    void releaseHandle(CacheHandle handle);

    /**
     * 获取缓存命中统计
     */
    CacheStats getStats();

    /**
     * 修剪缓存到指定大小
     */
    void pruneTo(long targetSize);
}
