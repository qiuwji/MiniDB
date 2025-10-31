package com.qiu.wal;

import com.qiu.util.CRC32;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * 日志写入器，负责将记录写入WAL文件
 */
public class LogWriter implements AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private long blockOffset; // 当前块内的偏移量
    private int blockRemaining; // 当前块剩余空间
    private boolean closed;

    // 当前分片状态（用于跨块记录）
    private boolean inFragmentedRecord;
    private LogType currentFragmentType;

    public LogWriter(String filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");

        // 创建或打开文件，追加写入
        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        this.blockOffset = fileChannel.position() % LogConstants.BLOCK_SIZE;
        this.blockRemaining = LogConstants.BLOCK_SIZE - (int) blockOffset;
        this.closed = false;
        this.inFragmentedRecord = false;
    }

    /**
     * 添加一条记录到日志
     */
    public void addRecord(byte[] data) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(data, "Data cannot be null");

        if (data.length > LogConstants.MAX_RECORD_SIZE) {
            throw new IllegalArgumentException("Record too large: " + data.length);
        }

        int dataOffset = 0;
        int dataRemaining = data.length;
        boolean isFirstFragment = true;

        // 分片写入记录（如果需要跨块）
        while (dataRemaining > 0) {
            final int fragmentSpace = Math.min(dataRemaining, blockRemaining - LogConstants.HEADER_SIZE);
            final boolean isLastFragment = (fragmentSpace == dataRemaining);

            LogType type;
            if (isFirstFragment && isLastFragment) {
                type = LogType.FULL_TYPE;
            } else if (isFirstFragment) {
                type = LogType.FIRST_TYPE;
            } else if (isLastFragment) {
                type = LogType.LAST_TYPE;
            } else {
                type = LogType.MIDDLE_TYPE;
            }

            writePhysicalRecord(type, data, dataOffset, fragmentSpace);

            dataOffset += fragmentSpace;
            dataRemaining -= fragmentSpace;
            isFirstFragment = false;
        }
    }

    /**
     * 写入物理记录（一个分片）
     */
    private void writePhysicalRecord(LogType type, byte[] data, int offset, int length)
            throws IOException {

        // 检查是否需要新块（如果剩余空间不足以放 header）
        if (blockRemaining < LogConstants.HEADER_SIZE) {
            if (blockRemaining > 0) {
                byte[] padding = new byte[blockRemaining];
                fileChannel.write(ByteBuffer.wrap(padding));
            }
            blockOffset = 0;
            blockRemaining = LogConstants.BLOCK_SIZE;
        }

        // 计算 CRC（包含 type + data fragment）
        byte[] crcData = new byte[1 + length];
        crcData[0] = type.getValue();
        if (length > 0) {
            System.arraycopy(data, offset, crcData, 1, length);
        }
        int crc = CRC32.value(crcData);

        // 构建并写入记录头：crc(4) | length(2) | type(1)
        ByteBuffer header = ByteBuffer.allocate(LogConstants.HEADER_SIZE);
        header.putInt(crc);                 // 4 bytes
        header.putShort((short) length);    // 2 bytes
        header.put(type.getValue());        // 1 byte
        header.flip();                      // <- 非常重要：准备从头写出

        // 写入头部
        while (header.hasRemaining()) {
            fileChannel.write(header);
        }

        // 写入数据片段（如果有）
        if (length > 0) {
            ByteBuffer dataBuf = ByteBuffer.wrap(data, offset, length);
            while (dataBuf.hasRemaining()) {
                fileChannel.write(dataBuf);
            }
        }

        // 可按需控制 flush（不必每条都强制）
        fileChannel.force(false);

        // 更新块状态
        int recordSize = LogConstants.HEADER_SIZE + length;
        blockOffset += recordSize;
        blockRemaining -= recordSize;

        // 更新分片状态
        if (type == LogType.FULL_TYPE || type == LogType.LAST_TYPE) {
            inFragmentedRecord = false;
        } else {
            inFragmentedRecord = true;
            currentFragmentType = type;
        }
    }

    /**
     * 获取当前文件位置
     */
    public long getFilePosition() throws IOException {
        checkNotClosed();
        return fileChannel.position();
    }

    /**
     * 获取文件大小
     */
    public long getFileSize() throws IOException {
        checkNotClosed();
        return fileChannel.size();
    }

    /**
     * 强制刷盘
     */
    public void flush() throws IOException {
        checkNotClosed();
        fileChannel.force(false);
    }

    /**
     * 同步刷盘（更强制的方式）
     */
    public void sync() throws IOException {
        checkNotClosed();
        fileChannel.force(true);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                // 确保所有数据刷盘
                flush();

                // 如果正在分片记录中，写入一个结束标记
                if (inFragmentedRecord) {
                    writePhysicalRecord(LogType.LAST_TYPE, new byte[0], 0, 0);
                }
            } finally {
                fileChannel.close();
                closed = true;
            }
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("LogWriter is closed");
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public String getFilePath() {
        return filePath;
    }
}
