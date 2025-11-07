package com.qiu.net.concretecommand;

import com.qiu.core.WriteBatch;
import com.qiu.net.AbstractCommand;
import com.qiu.net.ClientSession;
import com.qiu.net.Protocol;

/**
 * GET命令 (修正版)
 */
public class GetCommand extends AbstractCommand {

    public GetCommand(byte[] key) {
        super(Protocol.GET, key, null);
    }

    @Override
    public void execute(ClientSession session) {
        try {
            byte[] value = session.getStore().get(key);

            session.sendBinaryResponse(value);

        } catch (Exception e) {
            session.sendError("GET failed: " + e.getMessage());
        }
    }

    @Override
    public void executeInBatch(WriteBatch batch) {
        throw new UnsupportedOperationException("GET command cannot be executed in batch");
    }
}