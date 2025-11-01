package com.qiu.wal;

/**
 * 日志相关常量
 */
public class LogConstants {
    private LogConstants() {} // 防止实例化

    // 块大小（32KB）
    public static final int BLOCK_SIZE = 32 * 1024;

    // 头部大小：CRC(4) + 长度(2) + 类型(1) = 7字节
    public static final int HEADER_SIZE = 4 + 2 + 1;

    // 最大记录大小（必须小于块大小）
    public static final int MAX_RECORD_SIZE = 30 * 1024; // 30KB，留出2KB余量
}