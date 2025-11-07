package com.qiu.cache;

import java.util.Arrays;
import java.util.Objects;

/**
 * 缓存条目
 */
public class CacheEntry {
    private final String key;
    private final byte[] data;
    private final long size;
    private final long createTime;
    private long lastAccessTime;
    private int accessCount;

    public CacheEntry(String key, byte[] data) {
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.data = Objects.requireNonNull(data, "Data cannot be null").clone(); // 防御性拷贝
        this.size = data.length;
        this.createTime = System.currentTimeMillis();
        this.lastAccessTime = createTime;
        this.accessCount = 1;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }

    public long getSize() {
        return size;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public int getAccessCount() {
        return accessCount;
    }

    /**
     * 记录访问
     */
    public void recordAccess() {
        this.lastAccessTime = System.currentTimeMillis();
        this.accessCount++;
    }

    /**
     * 获取数据而不记录访问（用于内部操作）
     */
    byte[] getDataInternal() {
        return data;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CacheEntry that = (CacheEntry) obj;
        return Objects.equals(key, that.key) && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return String.format("CacheEntry{key='%s', size=%d, accesses=%d}",
                key, size, accessCount);
    }
}
