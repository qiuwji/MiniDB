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
 * SSTable构建器
 */
public class TableBuilder implements AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private final int blockSize;
    private final Comparator<byte[]> comparator;
    private final BloomFilter filter;

    private BlockBuilder dataBlockBuilder;
    private BlockBuilder indexBlockBuilder;
    private BlockHandle pendingHandle;
    private byte[] lastKey;
    private long offset;
    private boolean closed;
    private int entryCount;

    public TableBuilder(String filePath) throws IOException {
        this(filePath, 4 * 1024, new BytewiseComparator(), 10);
    }

    public TableBuilder(String filePath, int blockSize, Comparator<byte[]> comparator, int bitsPerKey)
            throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.blockSize = blockSize;
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");
        this.filter = new BloomFilter(bitsPerKey);

        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);

        this.dataBlockBuilder = new BlockBuilder(blockSize, comparator);
        this.indexBlockBuilder = new BlockBuilder(blockSize, comparator);
        this.offset = 0;
        this.closed = false;
        this.entryCount = 0;
    }

    /**
     * 添加键值对到Table
     */
    public void add(byte[] key, byte[] value) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // 检查键的顺序
        if (lastKey != null && comparator.compare(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be in ascending order");
        }

        // 添加到布隆过滤器
        filter.add(key);

        // 添加到数据块
        dataBlockBuilder.add(key, value);
        entryCount++;

        // 如果数据块已满，刷写到文件
        if (dataBlockBuilder.remainingCapacity() < key.length + value.length + 24) {
            flushDataBlock();
        }

        lastKey = key.clone(); // 保存最后键的拷贝
    }

    /**
     * 完成Table构建
     */
    public void finish() throws IOException {
        checkNotClosed();

        // 刷写最后一个数据块
        if (!dataBlockBuilder.isEmpty()) {
            flushDataBlock();
        }

        // 写入元数据块（布隆过滤器）
        byte[] filterData = filter.getFilter();
        BlockHandle filterHandle = writeRawBlock(filterData);

        // 完成索引块
        byte[] indexData = indexBlockBuilder.finish();
        BlockHandle indexHandle = writeRawBlock(indexData);

        // 写入Footer
        writeFooter(new Footer(filterHandle, indexHandle));

        // 强制刷盘
        fileChannel.force(true);

        closed = true;
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

        // 添加到索引块
        if (lastKey != null) {
            byte[] handleEncoding = encodeBlockHandle(dataHandle);
            indexBlockBuilder.add(lastKey, handleEncoding);
        }

        // 重置数据块构建器
        dataBlockBuilder.reset();
    }

    /**
     * 写入原始块数据
     */
    private BlockHandle writeRawBlock(byte[] data) throws IOException {
        long blockOffset = offset;
        int blockSize = data.length;

        // 写入数据
        fileChannel.write(ByteBuffer.wrap(data));
        offset += blockSize;

        return new BlockHandle(blockOffset, blockSize);
    }

    /**
     * 写入Footer
     */
    private void writeFooter(Footer footer) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Footer.ENCODED_LENGTH);

        // 编码元数据索引句柄（简化）
        buffer.putLong(footer.getMetaIndexHandle().getOffset());
        buffer.putLong(footer.getMetaIndexHandle().getSize());

        // 编码数据索引句柄
        buffer.putLong(footer.getIndexHandle().getOffset());
        buffer.putLong(footer.getIndexHandle().getSize());

        // 写入魔数
        buffer.putLong(Footer.MAGIC_NUMBER);

        buffer.flip();
        fileChannel.write(buffer);
        offset += Footer.ENCODED_LENGTH;
    }

    /**
     * 编码BlockHandle
     */
    private byte[] encodeBlockHandle(BlockHandle handle) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(handle.getOffset());
        buffer.putLong(handle.getSize());
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

    public long getFileSize() throws IOException {
        return offset;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public String getFilePath() {
        return filePath;
    }
}
