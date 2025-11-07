package com.qiu.net.concretecommand;

import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;
import com.qiu.net.ClientSession;
import com.qiu.net.Protocol;

/**
 * 批处理取消命令 (修正版)
 */
public class BatchCancelCommand extends AbstractCommand {

    public BatchCancelCommand() {
        super(Protocol.BATCH_CANCEL, null, null);
    }

    @Override
    public void execute(ClientSession session) {
        if (!session.isInBatch()) {
            session.sendError("No batch in progress");
            return;
        }

        session.cancelBatch();
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        throw new UnsupportedOperationException("BATCH_CANCEL cannot be in batch");
    }
}