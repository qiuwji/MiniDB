package com.qiu.net.concretecommand;

import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;
import com.qiu.net.ClientSession;
import com.qiu.net.Protocol;

/**
 * PUT命令
 */
public class PutCommand extends AbstractCommand {

    public PutCommand(byte[] key, byte[] value) {
        super(Protocol.PUT, key, value);
    }

    @Override
    public void execute(ClientSession session) {
        try {
            session.getStore().put(key, value);
            session.sendResponse("OK");
        } catch (Exception e) {
            session.sendError("PUT failed: " + e.getMessage());
        }
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        batch.put(key, value);
    }
}
