package com.qiu.net;

import com.qiu.core.WriteBatch;

/**
 * 命令接口
 */
public interface Command {
    /**
     * 立即执行命令（非批处理模式）
     */
    void execute(ClientSession session);

    /**
     * 在批处理中执行命令
     */
    void executeInBatch(WriteBatch batch);

    /**
     * 获取命令的key
     */
    byte[] getKey();

    /**
     * 获取命令的value
     */
    byte[] getValue();

    /**
     * 获取命令类型
     */
    byte getCommandType();
}