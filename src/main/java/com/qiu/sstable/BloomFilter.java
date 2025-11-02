package com.qiu.sstable;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Objects;
import java.util.Random;

/**
 * 布隆过滤器，用于快速判断键是否可能存在于SSTable中（改进版）
 */
public class BloomFilter {
    private static final int VERSION = 1;
    private static final long SEED_BASE = 0xdeafbeefL;

    private BitSet bits;
    private int bitSize;
    private int hashCount;
    private int[] seeds;
    private final double targetFalsePositiveRate;
    private int elementCount;

    public BloomFilter(int bitsPerKey) {
        this(bitsPerKey, 1000, 0.01); // 默认1000个键，1%假阳性率
    }

    public BloomFilter(int bitsPerKey, int expectedKeys) {
        this(bitsPerKey, expectedKeys, 0.01);
    }

    public BloomFilter(int bitsPerKey, int expectedKeys, double targetFalsePositiveRate) {
        if (bitsPerKey <= 0) {
            throw new IllegalArgumentException("Bits per key must be positive");
        }
        if (expectedKeys <= 0) {
            throw new IllegalArgumentException("Expected keys must be positive");
        }
        if (targetFalsePositiveRate <= 0 || targetFalsePositiveRate >= 1) {
            throw new IllegalArgumentException("False positive rate must be between 0 and 1");
        }

        this.targetFalsePositiveRate = targetFalsePositiveRate;
        this.elementCount = 0;

        // 基于目标假阳性率计算最优参数
        this.bitSize = calculateOptimalBitSize(expectedKeys, targetFalsePositiveRate);
        this.hashCount = calculateOptimalHashCount(bitSize, expectedKeys);

        // 使用更高质量的种子
        this.seeds = generateHighQualitySeeds(hashCount);
        this.bits = new BitSet(bitSize);
    }

    /**
     * 计算最优位数组大小
     * 公式: m = -n * ln(p) / (ln(2))^2
     */
    private int calculateOptimalBitSize(int expectedKeys, double falsePositiveRate) {
        if (falsePositiveRate <= 0) {
            return Math.max(1, 10 * expectedKeys); // 默认10 bits/key
        }

        double ln2 = Math.log(2);
        double size = -expectedKeys * Math.log(falsePositiveRate) / (ln2 * ln2);
        return Math.max(1, (int) Math.ceil(size));
    }

    /**
     * 计算最优哈希函数数量
     * 公式: k = (m/n) * ln(2)
     */
    private int calculateOptimalHashCount(int bitSize, int expectedKeys) {
        if (expectedKeys == 0) return 1;

        double count = ((double) bitSize / expectedKeys) * Math.log(2);
        return Math.max(1, (int) Math.round(count));
    }

    /**
     * 生成高质量种子
     */
    private int[] generateHighQualitySeeds(int count) {
        int[] seeds = new int[count];
        Random random = new Random(SEED_BASE);

        for (int i = 0; i < count; i++) {
            seeds[i] = random.nextInt();
        }
        return seeds;
    }

    /**
     * 添加键到布隆过滤器
     */
    public void add(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");

        int[] hashes = computeHashes(key);
        for (int hash : hashes) {
            int index = Math.abs(hash % bitSize);
            bits.set(index);
        }
        elementCount++;
    }

    /**
     * 批量添加键
     */
    public void addAll(byte[][] keys) {
        Objects.requireNonNull(keys, "Keys array cannot be null");

        for (byte[] key : keys) {
            if (key != null) {
                add(key);
            }
        }
    }

    /**
     * 检查键是否可能存在于布隆过滤器中
     */
    public boolean mayContain(byte[] key) {
        Objects.requireNonNull(key, "Key cannot be null");

        int[] hashes = computeHashes(key);
        for (int hash : hashes) {
            int index = Math.abs(hash % bitSize);
            if (!bits.get(index)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 使用MurmurHash3计算多个哈希值
     */
    private int[] computeHashes(byte[] data) {
        int[] hashes = new int[hashCount];

        // 使用不同的种子生成多个哈希值
        for (int i = 0; i < hashCount; i++) {
            hashes[i] = murmurHash3(data, seeds[i]);
        }

        return hashes;
    }

    /**
     * MurmurHash3 32位实现
     */
    private int murmurHash3(byte[] data, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        final int r1 = 15;
        final int r2 = 13;
        final int m = 5;
        final int n = 0xe6546b64;

        int hash = seed;
        int length = data.length;
        int roundedEnd = (length & 0xfffffffc); // round down to 4 byte block

        // 处理4字节块
        for (int i = 0; i < roundedEnd; i += 4) {
            int k = ByteBuffer.wrap(data, i, 4).getInt();
            k *= c1;
            k = Integer.rotateLeft(k, r1);
            k *= c2;

            hash ^= k;
            hash = Integer.rotateLeft(hash, r2);
            hash = hash * m + n;
        }

        // 处理剩余字节
        int k = 0;
        switch (length & 0x03) {
            case 3:
                k ^= (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k ^= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k ^= data[roundedEnd] & 0xff;
                k *= c1;
                k = Integer.rotateLeft(k, r1);
                k *= c2;
                hash ^= k;
        }

        // 最终混合
        hash ^= length;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;

        return hash;
    }

    /**
     * 获取布隆过滤器数据（用于序列化）
     */
    public byte[] getFilter() {
        byte[] bitsData = bits.toByteArray();

        // 计算所需缓冲区大小：版本(4) + 位大小(4) + 哈希数量(4) + 元素数量(4) + 种子 + 位数据
        int seedsSize = hashCount * 4;
        int totalSize = 16 + seedsSize + bitsData.length;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // 写入头部信息
        buffer.putInt(VERSION);              // 版本号
        buffer.putInt(bitSize);              // 位数组大小
        buffer.putInt(hashCount);            // 哈希函数数量
        buffer.putInt(elementCount);         // 元素数量

        // 写入种子
        for (int seed : seeds) {
            buffer.putInt(seed);
        }

        // 写入位数据
        buffer.put(bitsData);

        // 添加CRC32校验和（可选，但推荐）
        buffer.flip();
        return buffer.array();
    }

    /**
     * 从序列化数据创建布隆过滤器
     */
    public static BloomFilter createFromFilter(byte[] filterData) {
        if (filterData == null || filterData.length < 16) {
            throw new IllegalArgumentException("Invalid filter data: too short");
        }

        ByteBuffer buffer = ByteBuffer.wrap(filterData);

        // 读取头部信息
        int version = buffer.getInt();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported filter version: " + version);
        }

        int bitSize = buffer.getInt();
        int hashCount = buffer.getInt();
        int elementCount = buffer.getInt();

        if (bitSize <= 0 || hashCount <= 0 || elementCount < 0) {
            throw new IllegalArgumentException("Invalid filter parameters");
        }

        // 读取种子
        int[] seeds = new int[hashCount];
        for (int i = 0; i < hashCount; i++) {
            seeds[i] = buffer.getInt();
        }

        // 读取位数据
        byte[] bitsData = new byte[buffer.remaining()];
        buffer.get(bitsData);

        // 创建过滤器实例
        BloomFilter filter = new BloomFilter(10, 1, 0.01); // 临时参数
        filter.bitSize = bitSize;
        filter.hashCount = hashCount;
        filter.elementCount = elementCount;
        filter.seeds = seeds;
        filter.bits = BitSet.valueOf(bitsData);

        return filter;
    }

    /**
     * 估算当前假阳性率
     */
    public double estimateFalsePositiveRate() {
        if (elementCount == 0) {
            return 0.0;
        }

        int bitCount = bits.cardinality();
        double ratio = (double) bitCount / bitSize;

        // 假阳性率公式: (1 - e^(-k * n / m)) ^ k
        double exponent = -hashCount * elementCount / (double) bitSize;
        return Math.pow(1 - Math.exp(exponent), hashCount);
    }

    /**
     * 获取位数组大小
     */
    public int getBitSize() {
        return bitSize;
    }

    /**
     * 获取哈希函数数量
     */
    public int getHashCount() {
        return hashCount;
    }

    /**
     * 获取元素数量
     */
    public int getElementCount() {
        return elementCount;
    }

    /**
     * 获取目标假阳性率
     */
    public double getTargetFalsePositiveRate() {
        return targetFalsePositiveRate;
    }

    /**
     * 获取实际设置的位数
     */
    public int getSetBitCount() {
        return bits.cardinality();
    }

    /**
     * 获取位数组使用率
     */
    public double getBitUsageRatio() {
        return (double) getSetBitCount() / bitSize;
    }

    /**
     * 清空过滤器
     */
    public void clear() {
        bits.clear();
        elementCount = 0;
    }

    /**
     * 检查过滤器是否为空
     */
    public boolean isEmpty() {
        return elementCount == 0;
    }

    /**
     * 获取过滤器内存占用估算（字节）
     */
    public int getMemoryUsage() {
        // BitSet.toByteArray() 返回的长度可以估算内存使用
        return bits.toByteArray().length + hashCount * 4 + 16; // + 头部开销
    }

    @Override
    public String toString() {
        return String.format("BloomFilter{bits=%d/%d, hashes=%d, elements=%d, fpr=%.4f}",
                getSetBitCount(), bitSize, hashCount, elementCount, estimateFalsePositiveRate());
    }

    /**
     * 创建基于目标假阳性率的布隆过滤器（推荐使用）
     */
    public static BloomFilter createWithFalsePositiveRate(int expectedKeys, double falsePositiveRate) {
        if (expectedKeys <= 0) {
            throw new IllegalArgumentException("Expected keys must be positive");
        }
        if (falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("False positive rate must be between 0 and 1");
        }

        // 计算所需的 bits per key
        double bitsPerKey = -Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2));
        return new BloomFilter((int) Math.ceil(bitsPerKey), expectedKeys, falsePositiveRate);
    }

    /**
     * 合并两个布隆过滤器（需要相同参数）
     */
    public static BloomFilter merge(BloomFilter filter1, BloomFilter filter2) {
        if (filter1.bitSize != filter2.bitSize || filter1.hashCount != filter2.hashCount) {
            throw new IllegalArgumentException("Cannot merge filters with different parameters");
        }

        // 检查种子是否相同
        for (int i = 0; i < filter1.hashCount; i++) {
            if (filter1.seeds[i] != filter2.seeds[i]) {
                throw new IllegalArgumentException("Cannot merge filters with different seeds");
            }
        }

        BloomFilter result = new BloomFilter(10, 1, 0.01); // 临时参数
        result.bitSize = filter1.bitSize;
        result.hashCount = filter1.hashCount;
        result.seeds = filter1.seeds;
        result.elementCount = filter1.elementCount + filter2.elementCount;

        // 合并位数组
        result.bits = (BitSet) filter1.bits.clone();
        result.bits.or(filter2.bits);

        return result;
    }
}