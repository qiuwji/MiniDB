package com.qiu.memory;

import java.util.Arrays;
import java.util.Objects;

/**
 * 内部键，包含用户键和序列号，用于支持多版本和删除标记
 */
public class InternalKey implements Comparable<InternalKey> {
    private final byte[] userKey;
    private final long sequence;
    private final ValueType valueType;

    public enum ValueType {
        VALUE((byte) 1),
        DELETION((byte) 0); // 墓碑标记

        private final byte value;

        ValueType(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static ValueType fromValue(byte value) {
            for (ValueType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown value type: " + value);
        }
    }

    public InternalKey(byte[] userKey, long sequence, ValueType valueType) {
        this.userKey = Objects.requireNonNull(userKey, "User key cannot be null").clone();
        this.sequence = sequence;
        this.valueType = Objects.requireNonNull(valueType, "Value type cannot be null");

        if (userKey.length == 0) {
            throw new IllegalArgumentException("User key cannot be empty");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("Sequence number cannot be negative");
        }
    }

    public byte[] getUserKey() {
        return userKey.clone(); // 防御性拷贝
    }

    public long getSequence() {
        return sequence;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public boolean isDeletion() {
        return valueType == ValueType.DELETION;
    }

    /**
     * 将InternalKey编码为字节数组用于存储
     */
    public byte[] encode() {
        byte[] encoded = new byte[userKey.length + 1 + 8]; // userKey + type + sequence
        System.arraycopy(userKey, 0, encoded, 0, userKey.length);
        encoded[userKey.length] = valueType.getValue();

        // 以大端序编码序列号
        long seq = sequence;
        for (int i = 7; i >= 0; i--) {
            encoded[userKey.length + 1 + i] = (byte) (seq & 0xFF);
            seq >>>= 8;
        }

        return encoded;
    }

    /**
     * 从编码的字节数组解码InternalKey
     */
    public static InternalKey decode(byte[] encoded) {
        if (encoded.length < 9) { // 至少1字节key + 1字节type + 8字节sequence
            throw new IllegalArgumentException("Encoded data too short");
        }

        int keyLength = encoded.length - 9;
        byte[] userKey = new byte[keyLength];
        System.arraycopy(encoded, 0, userKey, 0, keyLength);

        ValueType valueType = ValueType.fromValue(encoded[keyLength]);

        long sequence = 0;
        for (int i = 0; i < 8; i++) {
            sequence = (sequence << 8) | (encoded[keyLength + 1 + i] & 0xFF);
        }

        return new InternalKey(userKey, sequence, valueType);
    }

    @Override
    public int compareTo(InternalKey other) {
        // 先按用户键升序排列
        int keyCompare = compareByteArrays(this.userKey, other.userKey);
        if (keyCompare != 0) {
            return keyCompare;
        }

        // 相同用户键按序列号降序排列（新的在前面）
        return Long.compare(other.sequence, this.sequence);
    }

    private int compareByteArrays(byte[] a, byte[] b) {
        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        InternalKey that = (InternalKey) obj;
        return sequence == that.sequence &&
                valueType == that.valueType &&
                Arrays.equals(userKey, that.userKey);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(userKey);
        result = 31 * result + Long.hashCode(sequence);
        result = 31 * result + valueType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("InternalKey{key=%s, seq=%d, type=%s}",
                Arrays.toString(userKey), sequence, valueType);
    }
}
