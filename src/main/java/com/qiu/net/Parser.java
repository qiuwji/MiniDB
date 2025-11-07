package com.qiu.net;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * 协议解析器
 * 协议格式: | CommandType(1B) | KeyLen(4B) | ValLen(4B) | key | value |
 */
public class Parser {

    /**
     * 从输入流解析命令
     */
    public Command parse(InputStream input) throws IOException {
        DataInputStream dis = new DataInputStream(input);

        // 读取命令头部
        byte opType = dis.readByte();
        int keyLen = dis.readInt();
        int valLen = dis.readInt();

        // 读取key数据
        byte[] key = new byte[keyLen];
        if (keyLen > 0) {
            dis.readFully(key);
        }

        // 读取value数据
        byte[] value = new byte[valLen];
        if (valLen > 0) {
            dis.readFully(value);
        }

        // 通过命令工厂创建命令对象
        return CommandFactory.createCommand(opType, key, value);
    }

    /**
     * 验证命令格式是否合法
     */
    public boolean validateCommand(byte opType, int keyLen, int valLen) {
        return switch (opType) {
            case Protocol.PUT -> keyLen > 0 && valLen >= 0;  // PUT必须有key，value可以为空

            case Protocol.DELETE, Protocol.GET -> keyLen > 0 && valLen == 0;  // DELETE/GET必须有key，无value

            case Protocol.BATCH_START, Protocol.BATCH_COMMIT, Protocol.BATCH_CANCEL ->
                    keyLen == 0 && valLen == 0;  // 控制命令无key/value

            default -> false;  // 未知命令类型
        };
    }

    /**
     * 获取命令名称（用于日志和调试）
     */
    public static String getCommandName(byte opType) {
        switch (opType) {
            case Protocol.PUT: return "PUT";
            case Protocol.DELETE: return "DELETE";
            case Protocol.GET: return "GET";
            case Protocol.BATCH_START: return "BATCH_START";
            case Protocol.BATCH_COMMIT: return "BATCH_COMMIT";
            case Protocol.BATCH_CANCEL: return "BATCH_CANCEL";
            default: return "UNKNOWN(" + opType + ")";
        }
    }
}