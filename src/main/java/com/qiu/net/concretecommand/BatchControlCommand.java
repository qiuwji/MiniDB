package com.qiu.net.concretecommand;

import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;

/**
 * 批处理控制命令基类
 */
public abstract class BatchControlCommand extends AbstractCommand {

    public BatchControlCommand(byte commandType) {
        super(commandType, null, null);
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        throw new UnsupportedOperationException("Batch control commands cannot be executed in batch");
    }
}