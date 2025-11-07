package com.qiu.sstable;

import com.qiu.util.BytewiseComparator;
// === 修改点: 导入 BlockCache ===
import com.qiu.cache.BlockCache;

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
 * SSTable读取器（改进版）
 */
public class SSTable implements AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private final Footer footer;
    private final BloomFilter filter;
    private final Block indexBlock;
    private final Comparator<byte[]> comparator;

    // === 修改点: 新增缓存字段 ===
    private final BlockCache blockCache;
    private final long versionId; // 用于缓存键的唯一性
    // === 结束修改 ===

    private boolean closed;

    // === 修改点: 链式调用到主构造函数 ===
    public SSTable(String filePath) throws IOException {
        this(filePath, new BytewiseComparator(), null, 0L);
    }

    // === 修改点: 链式调用到主构造函数 ===
    public SSTable(String filePath, Comparator<byte[]> comparator) throws IOException {
        this(filePath, comparator, null, 0L);
    }

    /**
     * === 修改点: 新增的主构造函数，支持依赖注入 ===
     */
    public SSTable(String filePath, Comparator<byte[]> comparator, BlockCache cache, long versionId) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");

        // 存储注入的依赖
        this.blockCache = cache;
        this.versionId = versionId;

        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.footer = readFooter();
        this.filter = readFilter();

        // readIndexBlock() 现在会自动使用带缓存的 readBlock()
        this.indexBlock = readIndexBlock();
        this.closed = false;
    }


    /**
     * 查找指定键的值
     */
    public Optional<byte[]> get(byte[] key) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        if (!filter.mayContain(key)) {
            return Optional.empty();
        }

        // 在索引块中查找包含该键的数据块
        BlockHandle dataHandle = findDataBlockHandle(key);
        if (dataHandle == null) {
            return Optional.empty();
        }

        // 读取数据块并在其中查找
        Block dataBlock = readBlock(dataHandle);
        byte[] value = dataBlock.get(key);

        return value != null ? Optional.of(value) : Optional.empty();
    }

    /**
     * 查找包含指定键的数据块句柄
     */
    private BlockHandle findDataBlockHandle(byte[] key) throws IOException {
        Block.BlockIterator indexIter = indexBlock.iterator();

        // 在索引块中查找第一个索引键 >= 目标键的数据块
        indexIter.seek(key);

        if (!indexIter.isValid()) {
            // 如果没找到，可能键在最后一个数据块
            return getLastDataBlockHandle();
        }

        // 获取找到的索引条目
        byte[] indexKey = indexIter.key();
        byte[] handleData = indexIter.value();
        BlockHandle currentHandle = decodeBlockHandle(handleData);

        // 检查是否是第一个数据块
        if (isFirstDataBlock(indexIter)) {
            // 第一个数据块包含所有 <= indexKey 的键
            // 因为 key000 <= key009，所以应该在第一个数据块中
            if (comparator.compare(key, indexKey) <= 0) {
                return currentHandle;
            } else {
                // key > 第一个索引键，说明不在任何数据块中
                return null;
            }
        }

        // 不是第一个数据块，需要获取前一个索引键
        byte[] prevIndexKey = getIndexKeyBefore(indexIter);
        if (prevIndexKey != null) {
            // 数据块的范围是: (prevIndexKey, currentIndexKey]
            // 例如：索引条目1的范围是 (key009, key019]
            if (comparator.compare(key, prevIndexKey) > 0 &&
                    comparator.compare(key, indexKey) <= 0) {
                return currentHandle;
            }
        }

        // 如果不在当前块，可能在前一个块
        return getPreviousDataBlockHandle(indexIter);
    }

    /**
     * 获取最后一个数据块的句柄
     */
    private BlockHandle getLastDataBlockHandle() throws IOException {
        Block.BlockIterator indexIter = indexBlock.iterator();
        indexIter.seekToFirst();

        if (!indexIter.isValid()) {
            return null;
        }

        // 移动到最后一个索引条目
        BlockHandle lastHandle = null;
        while (indexIter.isValid()) {
            byte[] handleData = indexIter.value();
            lastHandle = decodeBlockHandle(handleData);
            indexIter.next();
        }

        return lastHandle;
    }

    /**
     * 检查是否是第一个数据块
     */
    private boolean isFirstDataBlock(Block.BlockIterator indexIter) throws IOException {
        int currentPosition = indexIter.getCurrentEntry();
        return currentPosition == 0;
    }

    /**
     * 获取前一个数据块的句柄
     */
    private BlockHandle getPreviousDataBlockHandle(Block.BlockIterator indexIter) throws IOException {
        int currentPosition = indexIter.getCurrentEntry();
        if (currentPosition <= 0) {
            return null;
        }

        // 保存当前位置
        byte[] currentKey = indexIter.key();
        byte[] currentValue = indexIter.value();

        // 移动到前一个位置
        indexIter.seekToFirst();
        for (int i = 0; i < currentPosition - 1; i++) {
            if (!indexIter.isValid()) {
                break;
            }
            indexIter.next();
        }

        BlockHandle prevHandle = null;
        if (indexIter.isValid()) {
            byte[] handleData = indexIter.value();
            prevHandle = decodeBlockHandle(handleData);
        }

        // 恢复原位置
        indexIter.seek(currentKey);

        return prevHandle;
    }

    /**
     * 获取指定迭代器位置前一个索引键
     */
    private byte[] getIndexKeyBefore(Block.BlockIterator indexIter) throws IOException {
        int currentPosition = indexIter.getCurrentEntry();
        if (currentPosition <= 0) {
            return null;
        }

        // 保存当前位置
        byte[] currentKey = indexIter.key();
        byte[] currentValue = indexIter.value();

        // 移动到前一个位置
        indexIter.seekToFirst();
        for (int i = 0; i < currentPosition - 1; i++) {
            if (!indexIter.isValid()) {
                break;
            }
            indexIter.next();
        }

        byte[] prevKey = null;
        if (indexIter.isValid()) {
            prevKey = indexIter.key();
        }

        // 恢复原位置
        indexIter.seek(currentKey);

        return prevKey;
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
            throw new IOException("File too short for SSTable: " + fileSize + " bytes");
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
            throw new IOException("Invalid SSTable magic number: 0x" + Long.toHexString(magic));
        }

        // 验证句柄有效性
        if (metaIndexOffset < 0 || metaIndexSize < 0 || indexOffset < 0 || indexSize < 0) {
            throw new IOException("Invalid block handle in footer");
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
        if (filterHandle.size() == 0) {
            // 如果没有布隆过滤器，返回一个总是返回true的过滤器
            return createAlwaysTrueFilter();
        }

        byte[] filterData = readBlockData(filterHandle);
        return BloomFilter.createFromFilter(filterData);
    }

    /**
     * 创建总是返回true的布隆过滤器（用于无过滤器的情况）
     */
    private BloomFilter createAlwaysTrueFilter() {
        return new BloomFilter(1, 1) {
            @Override
            public boolean mayContain(byte[] key) {
                return true; // 总是返回true，不进行过滤
            }

            @Override
            public void add(byte[] key) {
                // 空实现
            }
        };
    }

    /**
     * 读取索引块
     */
    private Block readIndexBlock() throws IOException {
        BlockHandle indexHandle = footer.getIndexHandle();
        return readBlock(indexHandle); // <-- 新代码: 调用缓存版本的 readBlock
    }

    /**
     * 读取指定块的数据
     */
    Block readBlock(BlockHandle handle) throws IOException {
        // 1. 如果没有注入缓存，保持原逻辑
        if (blockCache == null) {
            byte[] blockData = readBlockData(handle);
            return new Block(blockData, comparator);
        }

        long blockOffset = handle.offset();

        // 2. 尝试从缓存读取
        // (使用 filePath 作为 tableName, blockOffset 和 versionId 作为唯一键)
        byte[] cachedData = blockCache.get(this.filePath, blockOffset, this.versionId);

        if (cachedData != null) {
            // 3. 缓存命中 (Cache Hit)
            // System.out.println("缓存命中");
            return new Block(cachedData, comparator);
        }

        // 4. 缓存未命中 (Cache Miss)
        // 从磁盘读取原始数据
        byte[] blockData = readBlockData(handle);

        // 5. 将数据存入缓存
        // (BlockCache 已修改为接受 byte[])
        blockCache.put(this.filePath, blockOffset, blockData, this.versionId);

        return new Block(blockData, comparator);
    }

    /**
     * 读取块原始数据 (此方法不变，作为缓存未命中时的回退)
     */
    private byte[] readBlockData(BlockHandle handle) throws IOException {
        // 允许大小为0的块（空块）
        if (handle.size() < 0) {
            throw new IOException("Invalid block size: " + handle.size());
        }

        if (handle.size() == 0) {
            // 返回空数组而不是抛出异常
            return new byte[0];
        }

        if (handle.offset() < 0) {
            throw new IOException("Invalid block offset: " + handle.offset());
        }

        long fileSize = fileChannel.size();
        if (handle.offset() + handle.size() > fileSize) {
            throw new IOException("Block extends beyond file end. Offset: " +
                    handle.offset() + ", Size: " + handle.size() + ", File: " + fileSize);
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) handle.size());
        int bytesRead = fileChannel.read(buffer, handle.offset());

        if (bytesRead != handle.size()) {
            throw new IOException("Failed to read complete block. Expected: " +
                    handle.size() + ", Actual: " + bytesRead);
        }

        return buffer.array();
    }

    /**
     * 解码BlockHandle
     */
    BlockHandle decodeBlockHandle(byte[] data) {
        if (data.length != 16) {
            throw new IllegalArgumentException("Invalid block handle data length: " + data.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        long offset = buffer.getLong();
        long size = buffer.getLong();

        if (offset < 0 || size < 0) {
            throw new IllegalArgumentException("Invalid block handle: offset=" + offset + ", size=" + size);
        }

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
     * 获取SSTable的统计信息
     */
    public TableStats getStats() throws IOException {
        int dataBlockCount = 0;
        Block.BlockIterator indexIter = indexBlock.iterator();
        indexIter.seekToFirst();

        while (indexIter.isValid()) {
            dataBlockCount++;
            indexIter.next();
        }

        return new TableStats(dataBlockCount, fileChannel.size());
    }

    /**
     * SSTable统计信息
     */
    public static class TableStats {
        private final int dataBlockCount;
        private final long fileSize;

        public TableStats(int dataBlockCount, long fileSize) {
            this.dataBlockCount = dataBlockCount;
            this.fileSize = fileSize;
        }

        public int getDataBlockCount() {
            return dataBlockCount;
        }

        public long getFileSize() {
            return fileSize;
        }

        @Override
        public String toString() {
            return String.format("TableStats{blocks=%d, size=%.2fKB}",
                    dataBlockCount, fileSize / 1024.0);
        }
    }

    /**
     * SSTable迭代器（统一实现）
     */
    public static class TableIterator {
        private final SSTable table;
        private Block.BlockIterator currentBlockIter;
        private Block.BlockIterator indexIter;
        private boolean valid;

        public TableIterator(SSTable table) throws IOException {
            this.table = table;
            this.indexIter = table.indexBlock.iterator();
            this.valid = false;

            // 定位到第一个数据块
            seekToFirst();
        }

        public boolean isValid() {
            return valid;
        }

        public void seekToFirst() throws IOException {
            indexIter.seekToFirst();
            if (indexIter.isValid()) {
                loadCurrentBlock();
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

            // 在索引块中查找包含目标键的数据块
            indexIter.seek(target);
            if (!indexIter.isValid()) {
                // 如果没找到，尝试最后一个数据块
                indexIter.seekToFirst();
                while (indexIter.isValid()) {
                    indexIter.next();
                }
                indexIter.seekToFirst(); // 重置到开始，然后移动到最后一个
                // 这里需要更复杂的逻辑来找到正确的数据块
                valid = false;
                return;
            }

            // 加载数据块并在其中查找
            loadCurrentBlock();
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

            loadCurrentBlock();
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
         * 加载当前索引条目对应的数据块
         */
        private void loadCurrentBlock() throws IOException {
            if (!indexIter.isValid()) {
                currentBlockIter = null;
                return;
            }

            byte[] handleData = indexIter.value();
            BlockHandle handle = table.decodeBlockHandle(handleData);
            // === 修改点: table.readBlock() 现在带缓存 ===
            Block block = table.readBlock(handle);
            currentBlockIter = block.iterator();
        }
    }

    // 测试用
    public Block getIndexBlock() {
        return indexBlock;
    }
}