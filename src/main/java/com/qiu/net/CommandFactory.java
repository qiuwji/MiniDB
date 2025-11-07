package com.qiu.net;

import com.qiu.net.concretecommand.*;

/**
 * 命令工厂类
 */
public class CommandFactory {

    public static Command createCommand(byte opType, byte[] key, byte[] value) {
        switch (opType) {
            case Protocol.PUT:
                return new PutCommand(key, value);

            case Protocol.DELETE:
                return new DeleteCommand(key);

            case Protocol.GET:
                return new GetCommand(key);

            case Protocol.BATCH_START:
                return new BatchStartCommand();

            case Protocol.BATCH_COMMIT:
                return new BatchCommitCommand();

            case Protocol.BATCH_CANCEL:
                return new BatchCancelCommand();

            default:
                throw new IllegalArgumentException("Unknown command type: " + opType);
        }
    }
}