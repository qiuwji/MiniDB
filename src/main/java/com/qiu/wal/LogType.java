package com.qiu.wal;

/**
 * 日志记录类型
 */
public enum LogType {
    ZERO_TYPE(0),      // 预分配类型
    FULL_TYPE(1),      // 完整记录
    FIRST_TYPE(2),     // 第一个分片
    MIDDLE_TYPE(3),    // 中间分片
    LAST_TYPE(4);      // 最后一个分片

    private final byte value;

    LogType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }

    public static LogType fromValue(byte value) {
        for (LogType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown log type: " + value);
    }
}
