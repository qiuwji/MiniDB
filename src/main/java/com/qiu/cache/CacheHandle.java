package com.qiu.cache;

import java.util.Objects;

/**
 * 缓存句柄，用于安全地访问缓存条目
 */
public class CacheHandle implements AutoCloseable {
    private final CacheEntry entry;
    private final Cache cache;
    private boolean closed;

    public CacheHandle(CacheEntry entry, Cache cache) {
        this.entry = Objects.requireNonNull(entry, "Entry cannot be null");
        this.cache = Objects.requireNonNull(cache, "Cache cannot be null");
        this.closed = false;

        // 记录访问
        entry.recordAccess();
    }

    public CacheEntry getEntry() {
        checkNotClosed();
        return entry;
    }

    public String getKey() {
        checkNotClosed();
        return entry.getKey();
    }

    public byte[] getData() {
        checkNotClosed();
        return entry.getData(); // 返回防御性拷贝
    }

    public long getSize() {
        checkNotClosed();
        return entry.getSize();
    }

    /**
     * 释放缓存句柄
     */
    @Override
    public void close() {
        if (!closed) {
            cache.releaseHandle(this);
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("CacheHandle is closed");
        }
    }

    public boolean isClosed() {
        return closed;
    }
}