package com.qiu.wal;

import com.qiu.util.CRC32;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Objects;

/**
 * 日志读取器，负责从WAL文件读取记录
 * [已修改] 此版本增强了对日志损坏的容忍能力。
 */
public class LogReader implements Iterator<byte[]>, AutoCloseable {
    private final FileChannel fileChannel;
    private final String filePath;
    private final boolean verifyChecksum;
    private int currentBlockRemaining; // 当前块剩余字节
    private boolean eof;
    private boolean closed;

    // 分片记录状态
    private ByteBuffer fragmentBuffer;
    private boolean inFragmentedRecord;

    // [新增] 用于为损坏报告提供更多上下文
    private long currentBlockNumber;

    public LogReader(String filePath) throws IOException {
        this(filePath, true);
    }

    public LogReader(String filePath, boolean verifyChecksum) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.verifyChecksum = verifyChecksum;

        Path path = Path.of(filePath);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        // [修改] 缓冲区大小应至少为最大记录大小，以容纳分片
        this.fragmentBuffer = ByteBuffer.allocate(LogConstants.MAX_RECORD_SIZE);

        reset();
    }

    @Override
    public boolean hasNext() {
        if (closed || eof) {
            return false;
        }

        try {
            // [修改] hasNext 不应改变文件指针状态
            long originalPos = fileChannel.position();
            int originalBlockRemaining = currentBlockRemaining;
            long originalBlockNumber = currentBlockNumber;
            boolean originalInFragment = inFragmentedRecord;
            // 创建一个临时缓冲区副本以进行“窥视”
            ByteBuffer originalFragmentBuffer = ByteBuffer.allocate(fragmentBuffer.capacity());
            originalFragmentBuffer.put(fragmentBuffer.duplicate().flip()).flip();

            try {
                byte[] record = readRecordInternal(false);
                return record != null;
            } finally {
                // [修改] 无论如何都要恢复原始状态
                fileChannel.position(originalPos);
                currentBlockRemaining = originalBlockRemaining;
                currentBlockNumber = originalBlockNumber;
                inFragmentedRecord = originalInFragment;
                fragmentBuffer.clear();
                fragmentBuffer.put(originalFragmentBuffer);
            }
        } catch (IOException e) {
            eof = true; // 假设IO异常意味着无法再读取
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
                // readRecordInternal 返回 null 表示 eof
                eof = true;
                throw new java.util.NoSuchElementException("No more records");
            }
            return record;
        } catch (IOException e) {
            eof = true; // 假设IO异常意味着无法再读取
            throw new RuntimeException("Failed to read log record", e);
        }
    }

    /**
     * 内部读取记录方法
     */
    private byte[] readRecordInternal(boolean consume) throws IOException {
        checkNotClosed();

        // [修改] 如果处于分片中且 consume 为 false (hasNext)，则返回一个非 null 哨兵
        if (!consume && inFragmentedRecord) {
            return new byte[0]; // 表示 "有下一个"，但不是完整记录
        }

        // [修改] 如果 consume 为 false，我们只查看下一条记录
        if (!consume) {
            PhysicalRecord physicalRecord = readPhysicalRecord();
            if (physicalRecord == null) {
                eof = true;
                return null; // 真正到达文件末尾
            }
            // 根据记录类型判断是否会产生一个完整的逻辑记录
            switch (physicalRecord.type) {
                case FULL_TYPE:
                case LAST_TYPE:
                    return new byte[0]; // 会产生一个完整记录
                case FIRST_TYPE:
                case MIDDLE_TYPE:
                    // 这种情况有点复杂。如果这是 hasNext()，
                    // 它将开始一个新的分片，但 hasNext() 应该返回 true。
                    // readPhysicalRecord 已经前进了，我们必须在 hasNext() 中回滚。
                    return new byte[0];
                case ZERO_TYPE:
                    // readPhysicalRecord 永远不会返回 ZERO_TYPE，它会跳过
                default:
                    return null; // 损坏或未知
            }
        }

        // --- 以下是 consume = true (next() 方法) 的逻辑 ---

        while (true) {
            PhysicalRecord physicalRecord = readPhysicalRecord();
            if (physicalRecord == null) {
                eof = true;
                // [修改] EOF 处理：如果在EOF时仍有未完成的分片，返回它（可能是损坏的）
                if (inFragmentedRecord && fragmentBuffer.position() > 0) {
                    reportCorruption("Reached EOF with an incomplete fragmented record");
                    return getFragmentBufferContent();
                }
                return null; // 正常 EOF
            }

            LogType type = physicalRecord.type;
            byte[] data = physicalRecord.data;

            // [修改] readPhysicalRecord 内部已处理 ZERO_TYPE，这里不再需要检查

            switch (type) {
                case FULL_TYPE:
                    if (inFragmentedRecord) {
                        reportCorruption("Found FULL_TYPE record while in a fragmented record. Discarding previous fragments.");
                        resetFragmentState();
                    }
                    return data;

                case FIRST_TYPE:
                    if (inFragmentedRecord) {
                        reportCorruption("Found FIRST_TYPE record while already in a fragmented record. Discarding previous fragments.");
                    }
                    startNewFragment(data);
                    break; // 继续循环以查找 MIDDLE/LAST

                case MIDDLE_TYPE:
                    if (!inFragmentedRecord) {
                        // [FIXED] 丢弃孤立的 MIDDLE 块
                        reportCorruption("Found orphaned MIDDLE_TYPE record. Discarding.");
                        continue; // 跳过此物理记录
                    }
                    appendToFragment(data);
                    break; // 继续循环以查找 MIDDLE/LAST

                case LAST_TYPE:
                    if (!inFragmentedRecord) {
                        // [FIXED] 丢弃孤立的 LAST 块
                        reportCorruption("Found orphaned LAST_TYPE record. Discarding.");
                        continue; // 跳过此物理记录
                    }
                    appendToFragment(data);
                    return getFragmentBufferContent(); // 记录组装完毕，返回

                default:
                    // 理论上 readPhysicalRecord 不会返回未知类型，但作为保险
                    reportCorruption("Unknown record type encountered: " + type);
                    continue;
            }

            // 如果我们在这里，说明我们正在处理一个分片 (FIRST/MIDDLE)
            // 并且正在等待更多分片，所以继续循环
        }
    }

    /**
     * [重构] 读取物理记录。此方法现在包含损坏处理逻辑。
     * 它要么返回一个有效的 PhysicalRecord，要么返回 null (表示真实EOF)。
     * 它会自动跳过损坏的块或填充。
     */
    private PhysicalRecord readPhysicalRecord() throws IOException {
        while (true) {
            // 1. 处理块边界
            if (currentBlockRemaining < LogConstants.HEADER_SIZE) {
                if (currentBlockRemaining > 0) {
                    // 跳过当前块中剩余的、不足一个header的字节
                    fileChannel.position(fileChannel.position() + currentBlockRemaining);
                }

                // [修改] 检查是否真正到达文件末尾
                if (fileChannel.position() >= fileChannel.size()) {
                    return null; // 真实 EOF
                }

                // 开始一个新块
                currentBlockRemaining = LogConstants.BLOCK_SIZE;
                currentBlockNumber = fileChannel.position() / LogConstants.BLOCK_SIZE;
            }

            long recordStartPos = fileChannel.position();

            // 2. 读取 header
            ByteBuffer header = ByteBuffer.allocate(LogConstants.HEADER_SIZE);
            int bytesRead = fileChannel.read(header);

            if (bytesRead < 0) {
                return null; // 真实 EOF
            }

            // [FIXED] 处理文件在 header 处被截断
            if (bytesRead != LogConstants.HEADER_SIZE) {
                reportCorruption("Partial header read, file may be truncated. Skipping to next block.");
                skipCurrentBlock(recordStartPos);
                continue; // 重试下一个块
            }
            header.flip();

            // 3. 解析 header
            int crc = header.getInt();
            short length = header.getShort();
            byte typeValue = header.get();

            // 4. 验证类型
            if (typeValue < 0 || typeValue > LogType.values().length) {
                reportCorruption("Invalid record type: " + typeValue + ". Skipping to next block.");
                skipCurrentBlock(recordStartPos);
                continue; // 重试下一个块
            }
            LogType type = LogType.fromValue(typeValue);

            // 5. [FIXED] 高效处理 ZERO_TYPE (填充)
            if (type == LogType.ZERO_TYPE) {
                // 这是一个填充记录，意味着此块的剩余部分都应被忽略
                skipCurrentBlock(recordStartPos);
                continue; // 重试下一个块
            }

            // 6. 验证长度
            int availableSpace = currentBlockRemaining - LogConstants.HEADER_SIZE;
            if (length < 0 || length > availableSpace) {
                reportCorruption("Invalid record length: " + length + ". Available space: " + availableSpace + ". Skipping to next block.");
                skipCurrentBlock(recordStartPos);
                continue; // 重试下一个块
            }

            // 7. 读取数据
            byte[] data = new byte[length];
            if (length > 0) {
                ByteBuffer dataBuffer = ByteBuffer.wrap(data);
                int dataBytesRead = fileChannel.read(dataBuffer);
                if (dataBytesRead != length) {
                    // [FIXED] 数据读取不完整
                    reportCorruption("Partial data read, expected " + length + ", got " + dataBytesRead + ". Skipping to next block.");
                    skipCurrentBlock(recordStartPos);
                    continue; // 重试下一个块
                }
            }

            // 8. [FIXED] 验证 CRC
            if (verifyChecksum) {
                byte[] crcData = new byte[1 + length];
                crcData[0] = typeValue;
                System.arraycopy(data, 0, crcData, 1, length);
                int actualCrc = CRC32.value(crcData);
                if (actualCrc != crc) {
                    reportCorruption("CRC mismatch. Expected: " + crc + ", Got: " + actualCrc + ". Skipping to next block.");
                    skipCurrentBlock(recordStartPos);
                    continue; // 重试下一个块
                }
            }

            // 9. 成功！更新块状态并返回
            int recordSize = LogConstants.HEADER_SIZE + length;
            currentBlockRemaining -= recordSize;

            return new PhysicalRecord(type, data);
        }
    }

    /**
     * [新增] 辅助方法，用于跳过当前块的剩余部分。
     * @param recordStartPos 损坏记录的起始位置，用于回退
     */
    private void skipCurrentBlock(long recordStartPos) throws IOException {
        // 回退到我们开始读取这个（损坏的）记录的位置
        fileChannel.position(recordStartPos);

        // 跳过这个块的全部剩余字节
        fileChannel.position(recordStartPos + currentBlockRemaining);

        // 强制下一次 readPhysicalRecord 循环进入“新块”逻辑
        currentBlockRemaining = 0;
    }

    /**
     * 分片记录状态管理方法
     */
    private void startNewFragment(byte[] data) {
        inFragmentedRecord = true;
        fragmentBuffer.clear();
        if (data != null && data.length > 0) {
            // [修改] 检查缓冲区溢出
            if (fragmentBuffer.remaining() < data.length) {
                reportCorruption("Fragment buffer overflow detected. Discarding fragments.");
                resetFragmentState();
            } else {
                fragmentBuffer.put(data);
            }
        }
    }

    private void appendToFragment(byte[] data) {
        if (data != null && data.length > 0) {
            // [修改] 检查缓冲区溢出
            if (fragmentBuffer.remaining() < data.length) {
                reportCorruption("Fragment buffer overflow detected. Discarding fragments.");
                resetFragmentState();
            } else {
                fragmentBuffer.put(data);
            }
        }
    }

    private byte[] getFragmentBufferContent() {
        byte[] result = new byte[fragmentBuffer.position()];
        fragmentBuffer.flip();
        fragmentBuffer.get(result);
        resetFragmentState();
        return result;
    }

    private void resetFragmentState() {
        inFragmentedRecord = false;
        fragmentBuffer.clear();
    }

    /**
     * 重置读取器状态
     */
    public void reset() throws IOException {
        checkNotClosed();
        fileChannel.position(0);
        currentBlockRemaining = 0;
        currentBlockNumber = 0;
        eof = false;
        resetFragmentState();
    }

    /**
     * 报告数据损坏
     */
    private void reportCorruption(String message) {
        System.err.println("Log corruption in " + filePath +
                " (near block " + currentBlockNumber + "): " + message);
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