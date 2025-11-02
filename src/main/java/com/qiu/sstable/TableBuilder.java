package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Objects;

/**
 * SSTable构建器（改进版）
 */
public class TableBuilder implements AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private final int blockSize;
    private final Comparator<byte[]> comparator;
    private final BloomFilter filter;
    private final double blockFlushThreshold;

    private BlockBuilder dataBlockBuilder;
    private BlockBuilder indexBlockBuilder;
    private byte[] lastKey;
    private long offset;
    private boolean closed;
    private int entryCount;
    private int blockCount;

    public TableBuilder(String filePath) throws IOException {
        this(filePath, 4 * 1024, new BytewiseComparator(), 10, 0.85);
    }

    public TableBuilder(String filePath, int blockSize, Comparator<byte[]> comparator, int bitsPerKey)
            throws IOException {
        this(filePath, blockSize, comparator, bitsPerKey, 0.85);
    }

    public TableBuilder(String filePath, int blockSize, Comparator<byte[]> comparator,
                        int bitsPerKey, double blockFlushThreshold) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.blockSize = blockSize;
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");
        this.blockFlushThreshold = validateThreshold(blockFlushThreshold);
        this.filter = new BloomFilter(bitsPerKey);

        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);

        this.dataBlockBuilder = new BlockBuilder(blockSize, comparator);
        this.indexBlockBuilder = new BlockBuilder(blockSize * 2, comparator); // 索引块可以大一些
        this.offset = 0;
        this.closed = false;
        this.entryCount = 0;
        this.blockCount = 0;
    }

    private double validateThreshold(double threshold) {
        if (threshold <= 0 || threshold > 1.0) {
            throw new IllegalArgumentException("Block flush threshold must be between 0 and 1");
        }
        return threshold;
    }

    /**
     * 添加键值对到Table（改进版）
     */
    public void add(byte[] key, byte[] value) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // 检查键的顺序
        if (lastKey != null && comparator.compare(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be in ascending order. " +
                    "Last key: " + (lastKey != null ? new String(lastKey) : "null") +
                    ", current key: " + new String(key));
        }

        // 添加到布隆过滤器
        filter.add(key);
        entryCount++;

        // 尝试添加到当前数据块
        if (!dataBlockBuilder.tryAdd(key, value)) {
            // 如果添加失败，说明当前块已满，先刷新
            flushDataBlock();

            // 重置后重新尝试添加
            if (!dataBlockBuilder.tryAdd(key, value)) {
                // 如果单条记录就超过块大小，需要特殊处理
                handleOversizedEntry(key, value);
                lastKey = key.clone();
                return;
            }
        }

        // 检查是否应该提前刷新（基于使用率阈值）
        if (dataBlockBuilder.shouldFlush() ||
                dataBlockBuilder.getUsageRatio() > blockFlushThreshold) {
            flushDataBlock();
        }

        lastKey = key.clone();
    }

    /**
     * 处理超大数据条目
     */
    private void handleOversizedEntry(byte[] key, byte[] value) throws IOException {
        int singleEntrySize = estimateSingleEntrySize(key, value);

        if (singleEntrySize > blockSize * 2) {
            throw new IOException("Entry too large: " + singleEntrySize +
                    " bytes, maximum supported: " + blockSize * 2);
        }

        // 创建只包含这个超大条目的专用块
        BlockBuilder oversizedBuilder = BlockBuilder.createOversizedBuilder(key, value);
        oversizedBuilder.forceAdd(key, value);

        byte[] blockData = oversizedBuilder.finish();
        BlockHandle handle = writeRawBlock(blockData);

        // 添加到索引 - 使用键范围
        addIndexEntry(oversizedBuilder.getKeyRange(), handle);

        blockCount++;
    }

    /**
     * 估算单条记录大小
     */
    private int estimateSingleEntrySize(byte[] key, byte[] value) {
        return 12 + key.length + value.length + 8; // 基础格式 + 重启点开销
    }

    /**
     * 刷写数据块到文件
     */
    private void flushDataBlock() throws IOException {
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        // 完成数据块
        byte[] blockData = dataBlockBuilder.finish();

        // 写入数据块
        BlockHandle dataHandle = writeRawBlock(blockData);

        // 添加到索引块 - 使用键范围而不是最后一个键
        addIndexEntry(dataBlockBuilder.getKeyRange(), dataHandle);

        // 重置数据块构建器
        dataBlockBuilder.reset();
        blockCount++;
    }

    /**
     * 添加索引条目（使用键范围）
     */
    private void addIndexEntry(BlockBuilder.KeyRange keyRange, BlockHandle handle) throws IOException {
        if (keyRange == null) {
            return;
        }

        byte[] handleEncoding = encodeBlockHandle(handle);

        // 使用数据块的最后一个键作为索引键
        // 这样索引查找时：index_key[i-1] < target_key <= index_key[i] 表示在第i个数据块
        byte[] indexKey = keyRange.getLastKey();
        if (!indexBlockBuilder.tryAdd(indexKey, handleEncoding)) {
            // 如果索引块也满了，需要特殊处理（通常不会发生，因为索引块更大）
            throw new IOException("Index block overflow");
        }
    }

    /**
     * 完成Table构建
     */
    public void finish() throws IOException {
        checkNotClosed();

        // 刷写最后一个数据块（如果有）
        if (!dataBlockBuilder.isEmpty()) {
            flushDataBlock();
        }

        // 写入元数据块（布隆过滤器）- 确保不为空
        byte[] filterData = filter.getFilter();
        // 如果布隆过滤器数据为空，创建最小有效数据
        if (filterData.length == 0) {
            filterData = new byte[]{0, 0, 0, 0}; // 最小有效块
        }
        BlockHandle filterHandle = writeRawBlock(filterData);

        // 完成索引块 - 确保不为空
        byte[] indexData = indexBlockBuilder.finish();
        // 如果索引块为空，创建最小有效数据
        if (indexData.length == 0) {
            indexData = createEmptyBlockData();
        }
        BlockHandle indexHandle = writeRawBlock(indexData);

        // 写入Footer
        writeFooter(new Footer(filterHandle, indexHandle));

        fileChannel.force(true);
        closed = true;
    }

    /**
     * 创建空的但有效的块数据
     */
    private byte[] createEmptyBlockData() {
        // 创建一个最小的有效块：重启点数量为0
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0); // numRestarts = 0
        return buffer.array();
    }

    /**
     * 放弃构建（删除文件）
     */
    public void abandon() throws IOException {
        if (!closed) {
            close();
            // 删除未完成文件
            Path path = Path.of(filePath);
            java.nio.file.Files.deleteIfExists(path);
        }
    }

    /**
     * 写入原始块数据
     */
    private BlockHandle writeRawBlock(byte[] data) throws IOException {
        long blockOffset = offset;
        int blockSize = data.length;

        System.out.println("Writing block at offset: " + blockOffset + ", size: " + blockSize);

        // 写入数据
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int written = 0;
        while (buffer.hasRemaining()) {
            written += fileChannel.write(buffer, blockOffset + written);
        }

        if (written != blockSize) {
            throw new IOException("Failed to write complete block. Expected: " +
                    blockSize + ", Actual: " + written);
        }

        offset += written;
        System.out.println("Block written, new offset: " + offset);
        return new BlockHandle(blockOffset, blockSize);
    }

    /**
     * 写入Footer
     */
    private void writeFooter(Footer footer) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Footer.ENCODED_LENGTH);

        // 编码元数据索引句柄
        buffer.putLong(footer.getMetaIndexHandle().offset());
        buffer.putLong(footer.getMetaIndexHandle().size());

        // 编码数据索引句柄
        buffer.putLong(footer.getIndexHandle().offset());
        buffer.putLong(footer.getIndexHandle().size());

        // 写入魔数
        buffer.putLong(Footer.MAGIC_NUMBER);

        // 重置position到0，准备读取
        buffer.flip();

        System.out.println("Footer buffer - position: " + buffer.position() + ", limit: " + buffer.limit() + ", capacity: " + buffer.capacity());

        // 写入到当前offset位置（文件末尾）
        int written = 0;
        while (buffer.hasRemaining()) {
            written += fileChannel.write(buffer, offset + written);
        }

        if (written != Footer.ENCODED_LENGTH) {
            throw new IOException("Failed to write complete footer. Expected: " +
                    Footer.ENCODED_LENGTH + ", Actual: " + written);
        }

        System.out.println("Footer written: " + written + " bytes at offset: " + offset);
        offset += written;
    }

    /**
     * 编码BlockHandle
     */
    private byte[] encodeBlockHandle(BlockHandle handle) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(handle.offset());
        buffer.putLong(handle.size());
        return buffer.array();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } finally {
                closed = true;
            }
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("TableBuilder is closed");
        }
    }

    /**
     * 获取当前文件大小
     */
    public long getFileSize() throws IOException {
        return offset;
    }

    /**
     * 获取条目总数
     */
    public int getEntryCount() {
        return entryCount;
    }

    /**
     * 获取数据块数量
     */
    public int getBlockCount() {
        return blockCount;
    }

    /**
     * 获取文件路径
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * 获取布隆过滤器（用于测试）
     */
    public BloomFilter getFilter() {
        return filter;
    }

    /**
     * 获取当前数据块的使用率（用于监控）
     */
    public double getCurrentBlockUsage() {
        return dataBlockBuilder.getUsageRatio();
    }

    /**
     * 获取索引块的使用率（用于监控）
     */
    public double getIndexBlockUsage() {
        return indexBlockBuilder.getUsageRatio();
    }

    /**
     * 手动触发数据块刷新（用于测试和特殊场景）
     */
    public void flush() throws IOException {
        checkNotClosed();
        flushDataBlock();
    }

    /**
     * 获取构建统计信息
     */
    public BuildStats getStats() {
        return new BuildStats(entryCount, blockCount, dataBlockBuilder.entryCount());
    }

    /**
     * 构建统计信息
     */
    public static class BuildStats {
        private final int totalEntries;
        private final int totalBlocks;
        private final int currentBlockEntries;

        public BuildStats(int totalEntries, int totalBlocks, int currentBlockEntries) {
            this.totalEntries = totalEntries;
            this.totalBlocks = totalBlocks;
            this.currentBlockEntries = currentBlockEntries;
        }

        public int getTotalEntries() {
            return totalEntries;
        }

        public int getTotalBlocks() {
            return totalBlocks;
        }

        public int getCurrentBlockEntries() {
            return currentBlockEntries;
        }

        public double getAverageEntriesPerBlock() {
            return totalBlocks > 0 ? (double) totalEntries / totalBlocks : 0;
        }

        @Override
        public String toString() {
            return String.format("BuildStats{entries=%d, blocks=%d, avg=%.2f, current=%d}",
                    totalEntries, totalBlocks, getAverageEntriesPerBlock(), currentBlockEntries);
        }
    }
}