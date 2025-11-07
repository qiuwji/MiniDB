package com.qiu.net;

/**
 * 协议格式
 * |-- CommandType(1B) --| |-- KeyLen(4B) --| |-- ValLen(4B) --| |-- key --| |-- value --|
 */
public class Protocol {
    public static final byte PUT = 0x01;
    public static final byte DELETE = 0x02;
    public static final byte GET = 0x03;
    public static final byte BATCH_START = 0x10;
    public static final byte BATCH_COMMIT = 0x11;
    public static final byte BATCH_CANCEL = 0x12;
}
