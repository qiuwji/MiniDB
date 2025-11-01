package com.qiu.wal;

import com.qiu.core.WriteBatch;
import com.qiu.util.Env;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 预写日志管理器
 */
public class WAL implements AutoCloseable {
    private final String filePath;
    private LogWriter writer;
    private boolean closed;

    public WAL(String filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");
        this.writer = new LogWriter(filePath);
        this.closed = false;
    }

    /**
     * 写入批量操作到WAL
     */
    public void write(WriteBatch batch) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(batch, "Write batch cannot be null");

        if (batch.isEmpty()) {
            return;
        }

        // 序列化WriteBatch
        byte[] serialized = serializeWriteBatch(batch);

        // 检查序列化后的大小
        if (serialized.length > LogConstants.MAX_RECORD_SIZE) {
            throw new IOException("Serialized WriteBatch too large: " + serialized.length +
                    " bytes (max: " + LogConstants.MAX_RECORD_SIZE + ")");
        }

        // 写入日志
        writer.addRecord(serialized);
    }

    /**
     * 序列化WriteBatch - 修复边界检查
     */
    private byte[] serializeWriteBatch(WriteBatch batch) {
        // 计算所需缓冲区大小
        int size = 8; // 序列号（8字节）
        for (WriteBatch.WriteOp op : batch.getOperations()) {
            size += 1; // 操作类型（1字节）
            size += 4 + op.key.length; // 键长度 + 键数据
            if (!op.isDelete) {
                size += 4 + op.value.length; // 值长度 + 值数据
            }
        }

        // 检查大小限制
        if (size > LogConstants.MAX_RECORD_SIZE) {
            throw new IllegalStateException("WriteBatch too large: " + size + " bytes");
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);

        // [MODIFIED] 写入真实的序列号
        buffer.putLong(batch.getSequenceNumber());

        // 写入每个操作
        for (WriteBatch.WriteOp op : batch.getOperations()) {
            // 操作类型：0=删除，1=插入
            buffer.put((byte) (op.isDelete ? 0 : 1));

            // 写入键
            if (op.key.length > 0) {
                buffer.putInt(op.key.length);
                buffer.put(op.key);
            } else {
                throw new IllegalArgumentException("Key cannot be empty");
            }

            // 如果是插入操作，写入值
            if (!op.isDelete) {
                if (op.value != null && op.value.length > 0) {
                    buffer.putInt(op.value.length);
                    buffer.put(op.value);
                } else {
                    throw new IllegalArgumentException("Value cannot be null or empty for put operation");
                }
            }
        }

        return buffer.array();
    }

    /**
     * 从WAL恢复数据（增强错误处理）
     */
    public List<WriteBatch> recover() throws IOException {
        checkNotClosed();

        List<WriteBatch> batches = new ArrayList<>();

        if (!Env.fileExists(filePath)) {
            return batches;
        }

        int successful = 0;
        int failed = 0;

        try (LogReader reader = new LogReader(filePath, true)) { // 启用CRC校验
            reader.reset(); // 确保状态重置

            while (reader.hasNext()) {
                try {
                    byte[] record = reader.next();
                    if (record != null && record.length > 0) {
                        WriteBatch batch = deserializeWriteBatch(record);
                        if (batch != null && !batch.isEmpty()) {
                            batches.add(batch);
                            successful++;
                        }
                    }
                } catch (Exception e) {
                    failed++;
                    System.err.println("Failed to recover WAL record: " + e.getMessage());
                    // 继续恢复，不中断
                }
            }
        } catch (Exception e) {
            System.err.println("WAL recovery error: " + e.getMessage());
            // 返回已成功恢复的批次
        }

        System.out.println("WAL recovery: " + successful + " successful, " + failed + " failed");
        return batches;
    }

    /**
     * 反序列化WriteBatch - 增强错误处理
     */
    private WriteBatch deserializeWriteBatch(byte[] data) {
        if (data == null || data.length < 8) {
            System.err.println("Invalid WAL record: too short or null");
            return null;
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            WriteBatch batch = new WriteBatch();

            // [MODIFIED] 读取序列号
            long sequence = buffer.getLong();
            // [NEW] 将恢复的序列号存入 WriteBatch
            batch.setSequenceNumber(sequence);

            // 读取每个操作
            while (buffer.hasRemaining()) {
                // 检查操作类型
                if (buffer.remaining() < 1) break;
                byte opType = buffer.get();
                boolean isDelete = (opType == 0);

                // 读取键
                if (buffer.remaining() < 4) break;
                int keyLength = buffer.getInt();
                if (keyLength < 0 || keyLength > buffer.remaining()) {
                    System.err.println("Invalid key length: " + keyLength);
                    break;
                }

                byte[] key = new byte[keyLength];
                buffer.get(key);

                if (isDelete) {
                    batch.delete(key);
                } else {
                    // 读取值
                    if (buffer.remaining() < 4) break;
                    int valueLength = buffer.getInt();
                    if (valueLength < 0 || valueLength > buffer.remaining()) {
                        System.err.println("Invalid value length: " + valueLength);
                        break;
                    }

                    byte[] value = new byte[valueLength];
                    buffer.get(value);
                    batch.put(key, value);
                }
            }

            return batch;
        } catch (Exception e) {
            System.err.println("Failed to deserialize WriteBatch: " + e.getMessage());
            return null;
        }
    }

    /**
     * 强制刷盘
     */
    public void flush() throws IOException {
        checkNotClosed();
        writer.flush();
    }

    /**
     * 同步刷盘
     */
    public void sync() throws IOException {
        checkNotClosed();
        writer.sync();
    }

    /**
     * 获取WAL文件大小
     */
    public long getFileSize() throws IOException {
        checkNotClosed();
        return writer.getFileSize();
    }

    /**
     * 删除WAL文件
     */
    public void delete() throws IOException {
        close();
        if (Env.fileExists(filePath)) {
            Env.deleteFile(filePath);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (writer != null) {
                writer.close();
            }
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("WAL is closed");
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public String getFilePath() {
        return filePath;
    }
}