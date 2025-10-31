// src/main/java/com/qiu/wal/LogReader.java
package com.qiu.wal;

import com.qiu.util.CRC32;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Objects;

/**
 * 日志读取器，负责从WAL文件读取记录
 */
public class LogReader implements Iterator<byte[]>, AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private final boolean verifyChecksum;
    private long currentBlockStart; // 当前块的起始位置
    private int currentBlockRemaining; // 当前块剩余字节
    private boolean eof;
    private boolean closed;

    // 分片记录状态
    private ByteBuffer fragmentBuffer;
    private boolean inFragmentedRecord;

    public LogReader(String filePath) throws IOException {
        this(filePath, true);
    }

    public LogReader(String filePath, boolean verifyChecksum) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.verifyChecksum = verifyChecksum;

        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.currentBlockStart = 0;
        this.currentBlockRemaining = 0;
        this.eof = false;
        this.closed = false;
        this.inFragmentedRecord = false;
        this.fragmentBuffer = ByteBuffer.allocate(LogConstants.MAX_RECORD_SIZE);
    }

    @Override
    public boolean hasNext() {
        if (closed || eof) {
            return false;
        }

        try {
            long pos = fileChannel.position();    // 保存当前位置
            try {
                // 尝试 peek（不消费）
                byte[] record = readRecordInternal(false);
                // 任何非 null 表示文件中还有记录（包括空片段的 byte[0]）
                return record != null;
            } finally {
                // 恢复到原来位置，避免 peek 消耗数据
                fileChannel.position(pos);
            }
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public byte[] next() {
        if (closed) {
            throw new IllegalStateException("LogReader is closed");
        }
        if (eof) {
            throw new java.util.NoSuchElementException("No more records");
        }

        try {
            byte[] record = readRecordInternal(true);
            if (record == null) {
                throw new java.util.NoSuchElementException("No more records");
            }
            return record;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read log record", e);
        }
    }

    /**
     * 内部读取记录方法
     * @param consume 是否消费记录（true）或只是查看（false）
     */
    private byte[] readRecordInternal(boolean consume) throws IOException {
        checkNotClosed();

        while (true) {
            // 读取物理记录
            PhysicalRecord physicalRecord = readPhysicalRecord();
            if (physicalRecord == null) {
                eof = true;
                return null;
            }

            LogType type = physicalRecord.type;
            byte[] data = physicalRecord.data;

            // 跳过ZERO_TYPE记录（修复问题）
            if (type == LogType.ZERO_TYPE) {
                continue;
            }

            // 处理分片记录
            if (type == LogType.FULL_TYPE) {
                if (inFragmentedRecord) {
                    // 不应该在分片记录中遇到完整记录
                    reportCorruption("Full record in the middle of fragmented record");
                    inFragmentedRecord = false;
                    fragmentBuffer.clear();
                    continue;
                }
                return data;
            } else if (type == LogType.FIRST_TYPE) {
                if (inFragmentedRecord) {
                    reportCorruption("First record without finishing previous fragmented record");
                    fragmentBuffer.clear();
                }
                inFragmentedRecord = true;
                fragmentBuffer.clear();
                if (data != null) {
                    fragmentBuffer.put(data);
                }
            } else if (type == LogType.MIDDLE_TYPE) {
                if (!inFragmentedRecord) {
                    reportCorruption("Middle record without first record");
                    continue;
                }
                if (data != null) {
                    fragmentBuffer.put(data);
                }
            } else if (type == LogType.LAST_TYPE) {
                if (!inFragmentedRecord) {
                    reportCorruption("Last record without first record");
                    continue;
                }
                if (data != null) {
                    fragmentBuffer.put(data);
                }
                inFragmentedRecord = false;
                byte[] fullRecord = new byte[fragmentBuffer.position()];
                fragmentBuffer.flip();
                fragmentBuffer.get(fullRecord);
                fragmentBuffer.clear();
                return fullRecord;
            } else {
                reportCorruption("Unknown record type: " + type);
                continue;
            }

            // 如果不是完整记录且不消费，返回空数组表示有更多数据
            if (!consume) {
                return new byte[0];
            }
        }
    }

    /**
     * 读取物理记录（一个分片）
     */
    private PhysicalRecord readPhysicalRecord() throws IOException {
        // 跳过块尾填充
        while (currentBlockRemaining < LogConstants.HEADER_SIZE) {
            if (currentBlockRemaining > 0) {
                // 跳过填充字节
                fileChannel.position(fileChannel.position() + currentBlockRemaining);
            }
            currentBlockStart += LogConstants.BLOCK_SIZE;
            currentBlockRemaining = LogConstants.BLOCK_SIZE;

            // 检查是否到达文件末尾
            if (fileChannel.position() >= fileChannel.size()) {
                return null;
            }
        }

        // 读取记录头
        ByteBuffer header = ByteBuffer.allocate(LogConstants.HEADER_SIZE);
        int headerBytesRead = fileChannel.read(header);
        if (headerBytesRead != LogConstants.HEADER_SIZE) {
            if (headerBytesRead < 0) {
                return null; // EOF
            }
            throw new EOFException("Partial header read");
        }
        header.flip();

        // 解析头部
        int expectedCrc = header.getInt();
        short length = header.getShort();
        byte typeValue = header.get();

        // 安全处理未知类型
        LogType type;
        try {
            type = LogType.fromValue(typeValue);
        } catch (IllegalArgumentException e) {
            reportCorruption("Unknown record type: " + typeValue);
            return null;
        }

        // 验证长度
        if (length < 0 || length > currentBlockRemaining - LogConstants.HEADER_SIZE) {
            reportCorruption("Invalid record length: " + length);
            return null;
        }

        // 读取数据
        byte[] data = new byte[length];
        if (length > 0) {
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);
            int dataBytesRead = fileChannel.read(dataBuffer);
            if (dataBytesRead != length) {
                throw new EOFException("Partial data read");
            }
        }

        // 验证CRC（如果启用）
        if (verifyChecksum && length > 0) {
            byte[] crcData = new byte[1 + length];
            crcData[0] = typeValue;
            System.arraycopy(data, 0, crcData, 1, length);

            int actualCrc = CRC32.value(crcData);
            if (actualCrc != expectedCrc) {
                reportCorruption("CRC mismatch: expected=" + expectedCrc + ", actual=" + actualCrc);
                return null;
            }
        }

        // 更新块状态
        int recordSize = LogConstants.HEADER_SIZE + length;
        currentBlockRemaining -= recordSize;

        return new PhysicalRecord(type, data);
    }

    /**
     * 报告数据损坏
     */
    private void reportCorruption(String message) {
        System.err.println("Log corruption in " + filePath + ": " + message);
        // 在实际实现中，这里应该记录到日志系统
    }

    /**
     * 重置读取位置到文件开头
     */
    public void reset() throws IOException {
        checkNotClosed();
        fileChannel.position(0);
        currentBlockStart = 0;
        currentBlockRemaining = 0;
        eof = false;
        inFragmentedRecord = false;
        fragmentBuffer.clear();
    }

    /**
     * 获取当前读取位置
     */
    public long getCurrentPosition() throws IOException {
        checkNotClosed();
        return fileChannel.position();
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
            throw new IllegalStateException("LogReader is closed");
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public String getFilePath() {
        return filePath;
    }

    /**
     * 物理记录内部类
     */
    private static class PhysicalRecord {
        final LogType type;
        final byte[] data;

        PhysicalRecord(LogType type, byte[] data) {
            this.type = type;
            this.data = data;
        }
    }
}