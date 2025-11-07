package com.qiu.net.concretecommand;

import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;
import com.qiu.net.ClientSession;
import com.qiu.net.Protocol;

/**
 * DELETE命令 (修正版)
 */
public class DeleteCommand extends AbstractCommand {

    public DeleteCommand(byte[] key) {
        // DELETE 没有 value
        super(Protocol.DELETE, key, null);
    }

    @Override
    public void execute(ClientSession session) {
        try {
            session.getStore().delete(key);
            session.sendResponse("OK");
        } catch (Exception e) {
            session.sendError("DELETE failed: " + e.getMessage());
        }
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        batch.delete(key);
    }
}