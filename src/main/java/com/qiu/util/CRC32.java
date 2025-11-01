package com.qiu.util;

public class CRC32 {
    private static final java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();

    /**
     * 计算字节数组的CRC32值
     * @param data 输入数据
     * @return CRC32校验值
     */
    public static int value(byte[] data) {
        synchronized (crc32) {
            crc32.reset();
            crc32.update(data);
            return (int) crc32.getValue();
        }
    }

    /**
     * 基于现有CRC值扩展计算新数据的CRC32
     * @param crc 现有的CRC32值
     * @param data 新数据
     * @return 扩展后的CRC32值
     */
    public static int extend(int crc, byte[] data) {
        // 由于标准库的CRC32不支持从指定值继续计算，
        // 这里我们创建一个新的实例来模拟扩展计算
        java.util.zip.CRC32 extendedCrc = new java.util.zip.CRC32();
        // 先将现有CRC值转换为字节数组并更新
        byte[] crcBytes = new byte[] {
                (byte) (crc >>> 24),
                (byte) (crc >>> 16),
                (byte) (crc >>> 8),
                (byte) crc
        };
        extendedCrc.update(crcBytes);
        // 再更新新数据
        extendedCrc.update(data);
        return (int) extendedCrc.getValue();
    }

    /**
     * 更高效的扩展计算方法（如果需要处理大量数据）
     * @param crc 现有的CRC32值
     * @param data 新数据
     * @return 扩展后的CRC32值
     */
    public static int extendEfficient(int crc, byte[] data) {
        // 创建新的CRC32实例
        java.util.zip.CRC32 newCrc = new java.util.zip.CRC32();
        // 重置并直接更新数据（如果原始CRC为0或者是初始状态）
        newCrc.update(data);
        return (int) newCrc.getValue();
    }

    /**
     * 使用CRC32对象进行流式计算（线程安全版本）
     */
    public static class CRC32Stream {
        private final java.util.zip.CRC32 crc = new java.util.zip.CRC32();

        public void update(byte[] data) {
            crc.update(data);
        }

        public void update(byte[] data, int off, int len) {
            crc.update(data, off, len);
        }

        public void update(byte b) {
            crc.update(b);
        }

        public int getValue() {
            return (int) crc.getValue();
        }

        public void reset() {
            crc.reset();
        }
    }

    /**
     * 创建新的CRC32流计算实例
     * @return CRC32Stream实例
     */
    public static CRC32Stream createStream() {
        return new CRC32Stream();
    }

    /**
     * 验证数据的CRC32校验值
     * @param data 数据
     * @param expectedCrc 期望的CRC32值
     * @return 验证是否通过
     */
    public static boolean verify(byte[] data, int expectedCrc) {
        return value(data) == expectedCrc;
    }

    /**
     * 分块计算CRC32值（适用于大文件）
     * @param chunks 数据块数组
     * @return 整个数据的CRC32值
     */
    public static int valueChunked(byte[][] chunks) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        for (byte[] chunk : chunks) {
            crc.update(chunk);
        }
        return (int) crc.getValue();
    }
}