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
    private int blockOffset; // 当前块内的偏移量
    private int blockRemaining; // 当前块剩余空间
    private boolean closed;

    public LogWriter(String filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");

        // 创建或打开文件，追加写入
        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        // 初始化块状态
        long fileSize = fileChannel.size();
        this.blockOffset = (int) (fileSize % LogConstants.BLOCK_SIZE);
        this.blockRemaining = LogConstants.BLOCK_SIZE - blockOffset;
        this.closed = false;
    }

    /**
     * 添加一条记录到日志
     */
    public void addRecord(byte[] data) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(data, "Data cannot be null");


// 允许大记录分片
//        if (data.length > LogConstants.MAX_RECORD_SIZE) {
//            throw new IllegalArgumentException("Record too large: " + data.length);
//        }

        int dataOffset = 0;
        int dataRemaining = data.length;
        boolean isFirstFragment = true;

        // 分片写入记录
        while (dataRemaining > 0) {
            // 检查是否需要开始新块
            if (blockRemaining < LogConstants.HEADER_SIZE) {
                writePadding();
            }

            // 计算当前分片大小
            int availableSpace = blockRemaining - LogConstants.HEADER_SIZE;
            int fragmentLength = Math.min(dataRemaining, availableSpace);
            boolean isLastFragment = (dataRemaining == fragmentLength);

            // 确定记录类型
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

            // 写入分片
            writeFragment(type, data, dataOffset, fragmentLength);

            // 更新状态
            dataOffset += fragmentLength;
            dataRemaining -= fragmentLength;
            isFirstFragment = false;
        }
    }

    /**
     * 写入填充字节
     */
    private void writePadding() throws IOException {
        if (blockRemaining > 0) {
            byte[] padding = new byte[blockRemaining];
            fileChannel.write(ByteBuffer.wrap(padding));
        }
        blockOffset = 0;
        blockRemaining = LogConstants.BLOCK_SIZE;
    }

    /**
     * 写入记录分片
     */
    private void writeFragment(LogType type, byte[] data, int offset, int length) throws IOException {
        // 计算CRC（包括类型和数据）
        byte[] crcData = new byte[1 + length];
        crcData[0] = type.getValue();
        if (length > 0) {
            System.arraycopy(data, offset, crcData, 1, length);
        }
        int crc = CRC32.value(crcData);

        // 准备头部
        ByteBuffer header = ByteBuffer.allocate(LogConstants.HEADER_SIZE);
        header.putInt(crc);
        header.putShort((short) length);
        header.put(type.getValue());
        header.flip();

        // 写入头部
        fileChannel.write(header);

        // 写入数据
        if (length > 0) {
            ByteBuffer dataBuffer = ByteBuffer.wrap(data, offset, length);
            fileChannel.write(dataBuffer);
        }

        // 更新块状态
        int recordSize = LogConstants.HEADER_SIZE + length;
        blockOffset += recordSize;
        blockRemaining -= recordSize;
    }

    /**
     * 强制刷盘
     */
    public void flush() throws IOException {
        checkNotClosed();
        fileChannel.force(false);
    }

    /**
     * 同步刷盘
     */
    public void sync() throws IOException {
        checkNotClosed();
        fileChannel.force(true);
    }

    /**
     * 获取文件大小
     */
    public long getFileSize() throws IOException {
        checkNotClosed();
        return fileChannel.size();
    }

    /**
     * 获取当前文件位置
     */
    public long getFilePosition() throws IOException {
        checkNotClosed();
        return fileChannel.position();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                flush();
                // 确保块对齐
                if (blockRemaining < LogConstants.BLOCK_SIZE) {
                    writePadding();
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