package com.qiu.version;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SSTable文件元数据
 * (修复了线程安全问题)
 */
public class FileMetaData {

    private final long fileNumber;
    private final long fileSize;
    private final byte[] smallestKey;
    private final byte[] largestKey;

    // 它可能被多个线程（如 compaction 检查）并发访问
    private final AtomicInteger allowedSeeks;

    public FileMetaData(long fileNumber, long fileSize, byte[] smallestKey, byte[] largestKey) {
        this(fileNumber, fileSize, smallestKey, largestKey, 100); // 默认100次查找后触发压缩
    }

    public FileMetaData(long fileNumber, long fileSize, byte[] smallestKey, byte[] largestKey, int allowedSeeks) {
        if (fileNumber < 0) {
            throw new IllegalArgumentException("File number cannot be negative");
        }
        if (fileSize < 0) {
            throw new IllegalArgumentException("File size cannot be negative");
        }
        this.fileNumber = fileNumber;
        this.fileSize = fileSize;
        this.smallestKey = Objects.requireNonNull(smallestKey, "Smallest key cannot be null").clone();
        this.largestKey = Objects.requireNonNull(largestKey, "Largest key cannot be null").clone();
        if (compareKeys(this.smallestKey, this.largestKey) > 0) {
            throw new IllegalArgumentException("Smallest key must be <= largest key");
        }
        if (allowedSeeks < 0) {
            throw new IllegalArgumentException("Allowed seeks cannot be negative");
        }
        // === 修复 P6: 初始化 AtomicInteger ===
        this.allowedSeeks = new AtomicInteger(allowedSeeks);
    }

    public long getFileNumber() {
        return fileNumber;
    }

    public long getFileSize() {
        return fileSize;
    }

    public byte[] getSmallestKey() {
        return smallestKey.clone(); // 防御性拷贝
    }

    public byte[] getLargestKey() {
        return largestKey.clone(); // 防御性拷贝
    }

    /**
     * === 修复 P6: 线程安全 getter ===
     */
    public int getAllowedSeeks() {
        return allowedSeeks.get();
    }

    /**
     * === 修复 P6: 线程安全 setter ===
     */
    public void setAllowedSeeks(int allowedSeeks) {
        if (allowedSeeks < 0) {
            throw new IllegalArgumentException("Allowed seeks cannot be negative");
        }
        this.allowedSeeks.set(allowedSeeks);
    }

    /**
     * 检查键是否在文件范围内
     */
    public boolean containsKey(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");
        return compareKeys(key, smallestKey) >= 0 && compareKeys(key, largestKey) <= 0;
    }

    /**
     * 比较两个键（按字节序）
     */
    private int compareKeys(byte[] a, byte[] b) {
        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FileMetaData that = (FileMetaData) obj;
        return fileNumber == that.fileNumber &&
                fileSize == that.fileSize &&
                // === 修复 P6: 比较原子变量的值 ===
                allowedSeeks.get() == that.allowedSeeks.get() &&
                Arrays.equals(smallestKey, that.smallestKey) &&
                Arrays.equals(largestKey, that.largestKey);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(fileNumber);
        result = 31 * result + Long.hashCode(fileSize);
        result = 31 * result + Arrays.hashCode(smallestKey);
        result = 31 * result + Arrays.hashCode(largestKey);
        // === 修复 P6: 使用原子变量的值 ===
        result = 31 * result + allowedSeeks.get();
        return result;
    }

    @Override
    public String toString() {
        return String.format("FileMetaData{fileNumber=%d, fileSize=%d, smallest=%s, largest=%s}",
                fileNumber, fileSize, Arrays.toString(smallestKey), Arrays.toString(largestKey));
    }
}