package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * SSTable读取器
 */
public class SSTable implements AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private final Footer footer;
    private final BloomFilter filter;
    final Block indexBlock;
    private final Comparator<byte[]> comparator;
    private boolean closed;

    public SSTable(String filePath) throws IOException {
        this(filePath, new BytewiseComparator());
    }

    public SSTable(String filePath, Comparator<byte[]> comparator) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");

        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.footer = readFooter();
        this.filter = readFilter();
        this.indexBlock = readIndexBlock();
        this.closed = false;
    }

    /**
     * 查找指定键的值
     */
    public Optional<byte[]> get(byte[] key) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        // 先用布隆过滤器快速判断
        if (!filter.mayContain(key)) {
            return Optional.empty();
        }

        // 在索引块中查找数据块
        Block.BlockIterator indexIter = indexBlock.iterator();
        indexIter.seek(key);

        if (!indexIter.isValid()) {
            return Optional.empty();
        }

        // 解析数据块句柄
        byte[] handleData = indexIter.value();
        BlockHandle dataHandle = decodeBlockHandle(handleData);

        // 读取数据块
        Block dataBlock = readBlock(dataHandle);
        Block.BlockIterator dataIter = dataBlock.iterator();
        dataIter.seek(key);

        if (dataIter.isValid() && comparator.compare(dataIter.key(), key) == 0) {
            return Optional.of(dataIter.value());
        }

        return Optional.empty();
    }

    /**
     * 创建Table迭代器
     */
    public TableIterator iterator() throws IOException {
        checkNotClosed();
        return new TableIterator(this);
    }

    /**
     * 读取Footer
     */
    private Footer readFooter() throws IOException {
        long fileSize = fileChannel.size();
        if (fileSize < Footer.ENCODED_LENGTH) {
            throw new IOException("File too short for SSTable");
        }

        ByteBuffer buffer = ByteBuffer.allocate(Footer.ENCODED_LENGTH);
        fileChannel.read(buffer, fileSize - Footer.ENCODED_LENGTH);
        buffer.flip();

        // 读取元数据索引句柄
        long metaIndexOffset = buffer.getLong();
        long metaIndexSize = buffer.getLong();

        // 读取数据索引句柄
        long indexOffset = buffer.getLong();
        long indexSize = buffer.getLong();

        // 验证魔数
        long magic = buffer.getLong();
        if (magic != Footer.MAGIC_NUMBER) {
            throw new IOException("Invalid SSTable magic number");
        }

        return new Footer(
                new BlockHandle(metaIndexOffset, metaIndexSize),
                new BlockHandle(indexOffset, indexSize)
        );
    }

    /**
     * 读取布隆过滤器
     */
    private BloomFilter readFilter() throws IOException {
        BlockHandle filterHandle = footer.getMetaIndexHandle();
        byte[] filterData = readBlockData(filterHandle);
        return BloomFilter.createFromFilter(filterData);
    }

    /**
     * 读取索引块
     */
    private Block readIndexBlock() throws IOException {
        BlockHandle indexHandle = footer.getIndexHandle();
        byte[] indexData = readBlockData(indexHandle);
        return new Block(indexData, comparator);
    }

    /**
     * 读取指定块的数据
     */
    Block readBlock(BlockHandle handle) throws IOException {
        byte[] blockData = readBlockData(handle);
        return new Block(blockData, comparator);
    }

    /**
     * 读取块原始数据
     */
    private byte[] readBlockData(BlockHandle handle) throws IOException {
        if (handle.getSize() <= 0) {
            throw new IOException("Invalid block size: " + handle.getSize());
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) handle.getSize());
        int bytesRead = fileChannel.read(buffer, handle.getOffset());

        if (bytesRead != handle.getSize()) {
            throw new IOException("Failed to read complete block");
        }

        return buffer.array();
    }

    /**
     * 解码BlockHandle
     */
    BlockHandle decodeBlockHandle(byte[] data) {
        if (data.length != 16) {
            throw new IllegalArgumentException("Invalid block handle data");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        long offset = buffer.getLong();
        long size = buffer.getLong();

        return new BlockHandle(offset, size);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            fileChannel.close();
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("SSTable is closed");
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public Footer getFooter() {
        return footer;
    }

    public BloomFilter getFilter() {
        return filter;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * SSTable内部迭代器实现
     */
    public class TableIterator {
        private final SSTable table;
        private Block.BlockIterator currentBlockIter;
        private Block.BlockIterator indexIter;
        private boolean valid;

        public TableIterator(SSTable table) throws IOException {
            this.table = table;
            this.indexIter = table.indexBlock.iterator();
            this.valid = false;

            // 定位到第一个数据块
            if (indexIter.isValid()) {
                loadBlock(indexIter.value());
                if (currentBlockIter != null) {
                    currentBlockIter.seekToFirst();
                    valid = currentBlockIter.isValid();
                }
            }
        }

        public boolean isValid() {
            return valid;
        }

        public void seekToFirst() throws IOException {
            indexIter.seekToFirst();
            if (indexIter.isValid()) {
                loadBlock(indexIter.value());
                if (currentBlockIter != null) {
                    currentBlockIter.seekToFirst();
                    valid = currentBlockIter.isValid();
                } else {
                    valid = false;
                }
            } else {
                valid = false;
            }
        }

        public void seek(byte[] target) throws IOException {
            if (target == null) {
                seekToFirst();
                return;
            }

            // 在索引块中查找
            indexIter.seek(target);
            if (!indexIter.isValid()) {
                valid = false;
                return;
            }

            // 加载数据块
            loadBlock(indexIter.value());
            if (currentBlockIter != null) {
                currentBlockIter.seek(target);
                valid = currentBlockIter.isValid();
            } else {
                valid = false;
            }
        }

        public void next() throws IOException {
            if (!valid) {
                throw new NoSuchElementException();
            }

            currentBlockIter.next();
            if (currentBlockIter.isValid()) {
                return;
            }

            // 当前块已结束，移动到下一个块
            indexIter.next();
            if (!indexIter.isValid()) {
                valid = false;
                return;
            }

            loadBlock(indexIter.value());
            if (currentBlockIter != null) {
                currentBlockIter.seekToFirst();
                valid = currentBlockIter.isValid();
            } else {
                valid = false;
            }
        }

        public byte[] key() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentBlockIter.key();
        }

        public byte[] value() {
            if (!valid) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return currentBlockIter.value();
        }

        /**
         * 加载指定块句柄对应的数据块
         */
        private void loadBlock(byte[] handleData) throws IOException {
            BlockHandle handle = table.decodeBlockHandle(handleData);
            Block block = table.readBlock(handle);
            currentBlockIter = block.iterator();
        }
    }
}