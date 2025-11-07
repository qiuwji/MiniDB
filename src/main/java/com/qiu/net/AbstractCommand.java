package com.qiu.net;

/**
 * 抽象命令基类
 */
public abstract class AbstractCommand implements Command {
    protected byte[] key;
    protected byte[] value;
    protected byte commandType;

    public AbstractCommand(byte commandType, byte[] key, byte[] value) {
        this.commandType = commandType;
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public byte getCommandType() {
        return commandType;
    }
}