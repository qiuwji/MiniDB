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
            return; // 空批次不需要写入
        }

        // 序列化WriteBatch
        byte[] serialized = serializeWriteBatch(batch);

        // 写入日志
        writer.addRecord(serialized);
    }

    /**
     * 序列化WriteBatch
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

        ByteBuffer buffer = ByteBuffer.allocate(size);

        // 写入序列号（暂时为0，由DB分配）
        buffer.putLong(0);

        // 写入每个操作
        for (WriteBatch.WriteOp op : batch.getOperations()) {
            // 操作类型：0=删除，1=插入
            buffer.put((byte) (op.isDelete ? 0 : 1));

            // 写入键
            buffer.putInt(op.key.length);
            buffer.put(op.key);

            // 如果是插入操作，写入值
            if (!op.isDelete) {
                buffer.putInt(op.value.length);
                buffer.put(op.value);
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

        // 检查文件是否存在
        if (!Env.fileExists(filePath)) {
            return batches; // 文件不存在，返回空列表
        }

        try (LogReader reader = new LogReader(filePath, false)) {
            while (reader.hasNext()) {
                try {
                    byte[] record = reader.next();
                    if (record != null && record.length > 0) {
                        WriteBatch batch = deserializeWriteBatch(record);
                        if (batch != null && !batch.isEmpty()) {
                            batches.add(batch);
                        }
                    }
                } catch (java.util.NoSuchElementException ne) {
                    System.err.println("Failed to recover WAL record: " + ne.getMessage());
                    // 发生在 reader.next() 抛出 NoSuchElement 时，继续尝试下一个
                    continue;
                } catch (Exception e) {
                    System.err.println("Failed to recover WAL record: " + e.getMessage());
                    // 继续恢复下一条，不中断
                }
            }
        }

        System.out.println("Recovered " + batches.size() + " write batches from WAL");
        return batches;
    }

    /**
     * 反序列化WriteBatch（增强错误处理）
     */
    private WriteBatch deserializeWriteBatch(byte[] data) {
        if (data == null || data.length < 8) {
            return null;
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            WriteBatch batch = new WriteBatch();

            // 读取序列号（暂时忽略）
            long sequence = buffer.getLong();

            // 读取每个操作
            while (buffer.hasRemaining()) {
                byte opType = buffer.get();
                boolean isDelete = (opType == 0);

                // 安全检查：确保有足够的数据读取键长度
                if (buffer.remaining() < 4) break;

                // 读取键
                int keyLength = buffer.getInt();
                if (keyLength < 0 || keyLength > buffer.remaining()) {
                    break; // 无效的键长度
                }

                byte[] key = new byte[keyLength];
                buffer.get(key);

                if (isDelete) {
                    batch.delete(key);
                } else {
                    // 安全检查：确保有足够的数据读取值长度
                    if (buffer.remaining() < 4) break;

                    int valueLength = buffer.getInt();
                    if (valueLength < 0 || valueLength > buffer.remaining()) {
                        break; // 无效的值长度
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

    /**
     * 重命名WAL文件（用于日志滚动）
     */
    public void rename(String newPath) throws IOException {
        checkNotClosed();
        close(); // 先关闭当前writer

        if (Env.fileExists(filePath)) {
            Env.renameFile(filePath, newPath);
        }

        // 创建新的writer
        this.writer = new LogWriter(newPath);
        this.closed = false;
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