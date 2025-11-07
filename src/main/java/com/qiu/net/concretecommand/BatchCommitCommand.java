package com.qiu.net.concretecommand;

import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;
import com.qiu.net.ClientSession;
import com.qiu.net.Protocol;

/**
 * 批处理提交命令 (修正版)
 */
public class BatchCommitCommand extends AbstractCommand {

    public BatchCommitCommand() {
        super(Protocol.BATCH_COMMIT, null, null);
    }

    @Override
    public void execute(ClientSession session) {
        if (!session.isInBatch()) {
            session.sendError("No batch in progress");
            return;
        }

        try {
            session.commitBatch();

        } catch (Exception e) {
            session.sendError("Batch commit failed: " + e.getMessage());
        }
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        throw new UnsupportedOperationException("BATCH_COMMIT cannot be in batch");
    }
}