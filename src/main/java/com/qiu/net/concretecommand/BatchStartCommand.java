package com.qiu.net.concretecommand;


import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;
import com.qiu.net.ClientSession;
import com.qiu.net.Protocol;

/**
 * 批处理开始命令
 */
public class BatchStartCommand extends AbstractCommand {

    public BatchStartCommand() {
        super(Protocol.BATCH_START, null, null);
    }

    @Override
    public void execute(ClientSession session) {
        if (session.isInBatch()) {
            session.sendError("Batch already started");
            return;
        }
        session.startBatch();  // 改为调用 startBatch()
        // session.sendResponse("BATCH_STARTED"); // 移除，startBatch() 内部已发送
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        throw new UnsupportedOperationException("BATCH_START cannot be in batch");
    }
}