// src/main/java/com/qiu/core/Options.java
package com.qiu.core;

/**
 * 数据库配置选项
 */
public class Options {
    private final int memtableSize;
    private final int cacheSize;
    private final boolean createIfMissing;
    private final boolean errorIfExists;
    private final int maxOpenFiles;
    private final int blockSize;
    private final int writeBufferSize;
    private final int maxLevels; // 新增：最大层级数（Level 数量）

    private Options(Builder builder) {
        this.memtableSize = builder.memtableSize;
        this.cacheSize = builder.cacheSize;
        this.createIfMissing = builder.createIfMissing;
        this.errorIfExists = builder.errorIfExists;
        this.maxOpenFiles = builder.maxOpenFiles;
        this.blockSize = builder.blockSize;
        this.writeBufferSize = builder.writeBufferSize;
        this.maxLevels = builder.maxLevels;
    }

    // Getters
    public int getMemtableSize() { return memtableSize; }
    public int getCacheSize() { return cacheSize; }
    public boolean isCreateIfMissing() { return createIfMissing; }
    public boolean isErrorIfExists() { return errorIfExists; }
    public int getMaxOpenFiles() { return maxOpenFiles; }
    public int getBlockSize() { return blockSize; }
    public int getWriteBufferSize() { return writeBufferSize; }
    public int getMaxLevels() { return maxLevels; } // 新增 getter

    /**
     * Builder模式创建配置
     */
    public static class Builder {
        private int memtableSize = 4 * 1024 * 1024; // 4MB
        private int cacheSize = 8 * 1024 * 1024;   // 8MB
        private boolean createIfMissing = true;
        private boolean errorIfExists = false;
        private int maxOpenFiles = 1000;
        private int blockSize = 4 * 1024;          // 4KB
        private int writeBufferSize = 4 * 1024 * 1024; // 4MB
        private int maxLevels = 7; // 默认层级数（L0..L6）

        public Builder memtableSize(int size) {
            if (size <= 0) throw new IllegalArgumentException("Memtable size must be positive");
            this.memtableSize = size;
            return this;
        }

        public Builder cacheSize(int size) {
            if (size < 0) throw new IllegalArgumentException("Cache size cannot be negative");
            this.cacheSize = size;
            return this;
        }

        public Builder createIfMissing(boolean create) {
            this.createIfMissing = create;
            return this;
        }

        public Builder errorIfExists(boolean error) {
            this.errorIfExists = error;
            return this;
        }

        public Builder maxOpenFiles(int max) {
            if (max <= 0) throw new IllegalArgumentException("Max open files must be positive");
            this.maxOpenFiles = max;
            return this;
        }

        public Builder blockSize(int size) {
            if (size <= 0) throw new IllegalArgumentException("Block size must be positive");
            this.blockSize = size;
            return this;
        }

        public Builder writeBufferSize(int size) {
            if (size <= 0) throw new IllegalArgumentException("Write buffer size must be positive");
            this.writeBufferSize = size;
            return this;
        }

        /**
         * 设置最大层级数（层级数量），必须 >= 1
         */
        public Builder maxLevels(int levels) {
            if (levels <= 0) throw new IllegalArgumentException("Max levels must be positive");
            this.maxLevels = levels;
            return this;
        }

        public Options build() {
            return new Options(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Options defaultOptions() {
        return new Builder().build();
    }
}
