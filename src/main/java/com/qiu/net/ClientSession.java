package com.qiu.net;

import com.qiu.core.MiniDB;
import com.qiu.core.WriteBatch;
import com.qiu.net.concretecommand.DeleteCommand;
import com.qiu.net.concretecommand.PutCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 客户端会话 (已修复缓冲区管理)
 */
public class ClientSession {
    private final SocketChannel channel;
    private final MiniDB db;
    private ByteBuffer readBuffer;
    private final AtomicLong commandCounter;

    // 批处理状态
    private boolean inBatch = false;
    private List<Command> batchCommands = new ArrayList<>();
    private WriteBatch currentWriteBatch;
    private long batchStartTime = 0;
    private static final long BATCH_TIMEOUT_MS = 30000;

    // 会话信息
    private final String clientAddress;
    private final long sessionStartTime;

    public ClientSession(SocketChannel channel, MiniDB db) {
        this.channel = channel;
        this.db = db;
        // 分配缓冲区，保持在 "writing" 模式 (pos=0, limit=capacity)
        this.readBuffer = ByteBuffer.allocate(8192);
        this.commandCounter = new AtomicLong(0);
        this.clientAddress = channel.socket().getRemoteSocketAddress().toString();
        this.sessionStartTime = System.currentTimeMillis();
    }

    /**
     * (已修复) 添加数据到缓冲区
     */
    public void appendBuffer(ByteBuffer newData) {
        // 1. 检查是否需要扩容
        if (readBuffer.remaining() < newData.remaining()) {
            int newCapacity = Math.max(readBuffer.capacity() * 2, readBuffer.position() + newData.remaining());
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);

            // 复制旧数据 (从 0 到 pos)
            readBuffer.flip(); // 切换到读取模式
            newBuffer.put(readBuffer); // 将旧数据放入新缓冲区
            readBuffer = newBuffer; // 替换
        }

        // 2. 将新数据添加到缓冲区
        readBuffer.put(newData);
    }

    /**
     * (已修复) 从缓冲区解析命令
     */
    public Command parseCommand() {
        checkBatchTimeout();

        // 切换到读取模式，以便解析
        readBuffer.flip();
        readBuffer.mark(); // 标记当前位置，以便在数据不足时回滚

        try {
            // 1. 检查头部是否完整
            if (readBuffer.remaining() < 9) { // 头部最小长度
                readBuffer.reset(); // 数据不足，重置到标记位置
                readBuffer.compact(); // *** 关键：将未读数据移到开头，准备下次写入
                return null; // 返回 null，等待更多数据
            }

            byte opType = readBuffer.get();
            int keyLen = readBuffer.getInt();
            int valLen = readBuffer.getInt();

            // 2. 检查数据体是否完整
            if (readBuffer.remaining() < keyLen + valLen) {
                readBuffer.reset(); // 数据不足，重置到标记位置
                readBuffer.compact(); // *** 关键：将未读数据移到开头
                return null; // 返回 null，等待更多数据
            }

            // 读取key和value (此时数据保证是完整的)
            byte[] key = new byte[keyLen];
            if (keyLen > 0) {
                readBuffer.get(key);
            }

            byte[] value = new byte[valLen];
            if (valLen > 0) {
                readBuffer.get(value);
            }

            commandCounter.incrementAndGet();

            readBuffer.compact(); // *** 关键：消耗已读数据，准备下次写入
            return CommandFactory.createCommand(opType, key, value);

        } catch (Exception e) {
            // 捕获到异常 (例如 BufferUnderflow 或 Factory 错误)
            // 抛出，由 DBServer.processData 处理
            readBuffer.compact(); // *** 关键：确保即使失败也 compact
            throw new RuntimeException("Failed to parse command", e);
        }
    }


    /**
     * 执行命令
     */
    public void executeCommand(Command command) throws IOException {
        command.execute(this);
    }

    /**
     * 添加到批处理
     */
    public void addToBatch(Command command) {
        batchCommands.add(command);
    }

    /**
     * 开始批处理
     */
    public void startBatch() {
        if (inBatch) {
            throw new IllegalStateException("Batch already started");
        }
        inBatch = true;
        batchCommands.clear();
        currentWriteBatch = new WriteBatch();
        batchStartTime = System.currentTimeMillis();
        sendResponse("BATCH_STARTED");
    }

    /**
     * 提交批处理
     */
    public void commitBatch() throws IOException {
        if (!inBatch) {
            throw new IllegalStateException("No batch in progress");
        }

        try {
            // 将批处理命令应用到WriteBatch
            for (Command command : batchCommands) {
                if (command instanceof PutCommand) {
                    currentWriteBatch.put(command.getKey(), command.getValue());
                } else if (command instanceof DeleteCommand) {
                    currentWriteBatch.delete(command.getKey());
                }
            }

            // 原子性写入数据库
            db.write(currentWriteBatch);
            sendResponse("BATCH_COMMITTED: " + batchCommands.size() + " operations");

        } finally {
            cleanupBatch();
        }
    }

    /**
     * 取消批处理
     */
    public void cancelBatch() {
        if (!inBatch) {
            throw new IllegalStateException("No batch in progress");
        }
        cleanupBatch();
        sendResponse("BATCH_CANCELLED");
    }

    /**
     * 清理批处理状态
     */
    private void cleanupBatch() {
        inBatch = false;
        batchCommands.clear();
        currentWriteBatch = null;
        batchStartTime = 0;
    }

    /**
     * 检查批处理超时
     */
    public void checkBatchTimeout() {
        if (inBatch && System.currentTimeMillis() - batchStartTime > BATCH_TIMEOUT_MS) {
            System.out.println("Batch timeout for client: " + clientAddress);
            cleanupBatch();
            sendError("Batch timeout - auto cancelled");
        }
    }

    /**
     * 发送响应
     */
    public void sendResponse(String response) {
        try {
            byte[] data = (response + "\n").getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
            buffer.putInt(data.length);
            buffer.put(data);
            buffer.flip();

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        } catch (IOException e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }

    /**
     * 发送错误
     */
    public void sendError(String error) {
        sendResponse("ERROR: " + error);
    }

    /**
     * 发送二进制数据
     */
    public void sendBinaryResponse(byte[] data) throws IOException {
        if (data == null) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(-1);
            buffer.flip();
            channel.write(buffer);
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        if (inBatch) {
            cleanupBatch();
        }

        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing channel: " + e.getMessage());
        }

        System.out.println("Session ended: " + clientAddress +
                ", commands: " + commandCounter.get());
    }

    // Getter方法
    public MiniDB getDb() {
        return db;
    }

    /**
     * 为命令类提供的兼容方法
     */
    public MiniDB getStore() {
        return db;
    }

    public boolean isInBatch() {
        return inBatch;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public long getCommandCount() {
        return commandCounter.get();
    }

    public long getSessionDuration() {
        return System.currentTimeMillis() - sessionStartTime;
    }

    // (*** 新增 Getter ***)
    public SocketChannel getChannel() {
        return this.channel;
    }
}