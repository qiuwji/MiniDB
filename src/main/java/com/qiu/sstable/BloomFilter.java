package com.qiu.sstable;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Objects;

/**
 * 布隆过滤器，用于快速判断键是否可能存在于SSTable中
 */
public class BloomFilter {
    private BitSet bits;
    private int bitSize;
    private int hashCount;
    private int[] seeds;

    public BloomFilter(int bitsPerKey) {
        this(bitsPerKey, 1024); // 默认初始大小
    }

    public BloomFilter(int bitsPerKey, int expectedKeys) {
        if (bitsPerKey <= 0) {
            throw new IllegalArgumentException("Bits per key must be positive");
        }
        if (expectedKeys <= 0) {
            throw new IllegalArgumentException("Expected keys must be positive");
        }

        this.bitSize = Math.max(1, bitsPerKey * expectedKeys);
        this.hashCount = Math.max(1, (int) Math.round(bitsPerKey * Math.log(2)));
        this.bits = new BitSet(bitSize);
        this.seeds = generateSeeds(hashCount);
    }

    /**
     * 添加键到布隆过滤器
     */
    public void add(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");

        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, seeds[i]);
            int index = Math.abs(hash % bitSize);
            bits.set(index);
        }
    }

    /**
     * 检查键是否可能存在于布隆过滤器中
     */
    public boolean mayContain(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");

        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, seeds[i]);
            int index = Math.abs(hash % bitSize);
            if (!bits.get(index)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取布隆过滤器数据（用于序列化）
     */
    public byte[] getFilter() {
        byte[] bytes = bits.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 8);
        buffer.putInt(bitSize);
        buffer.putInt(hashCount);
        buffer.put(bytes);
        return buffer.array();
    }

    /**
     * 从序列化数据创建布隆过滤器
     */
    public static BloomFilter createFromFilter(byte[] filterData) {
        if (filterData == null || filterData.length < 8) {
            throw new IllegalArgumentException("Invalid filter data");
        }

        ByteBuffer buffer = ByteBuffer.wrap(filterData);
        int bitSize = buffer.getInt();
        int hashCount = buffer.getInt();

        byte[] bitsData = new byte[filterData.length - 8];
        buffer.get(bitsData);

        BloomFilter filter = new BloomFilter(1, 1); // 临时创建
        filter.bitSize = bitSize;
        filter.hashCount = hashCount;
        filter.bits = BitSet.valueOf(bitsData);
        filter.seeds = generateSeeds(hashCount);

        return filter;
    }

    /**
     * 生成哈希种子
     */
    private static int[] generateSeeds(int count) {
        int[] seeds = new int[count];
        for (int i = 0; i < count; i++) {
            seeds[i] = i * 131 + 17; // 简单的种子生成策略
        }
        return seeds;
    }

    /**
     * 简单的哈希函数
     */
    private int hash(byte[] data, int seed) {
        int hash = seed;
        for (byte b : data) {
            hash = hash * 31 + b;
        }
        return hash;
    }

    public int getBitSize() {
        return bitSize;
    }

    public int getHashCount() {
        return hashCount;
    }

    public int getApproximateElementCount() {
        int bitCount = bits.cardinality();
        return (int) (-bitSize * Math.log(1 - (double) bitCount / bitSize) / hashCount);
    }
}
