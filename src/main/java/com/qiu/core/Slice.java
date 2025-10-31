package com.qiu.core;

import java.util.Arrays;
import java.util.Objects;

/**
 * 字节切片，包装字节数组并提供比较等功能
 */
public class Slice implements Comparable<Slice> {
    private final byte[] data;
    private final int offset;
    private final int length;

    public Slice(byte[] data) {
        this(data, 0, data.length);
    }

    public Slice(byte[] data, int offset, int length) {
        Objects.requireNonNull(data, "Data cannot be null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("Invalid offset or length");
        }
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getData() {
        return Arrays.copyOfRange(data, offset, offset + length);
    }

    public byte byteAt(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Length: " + length);
        }
        return data[offset + index];
    }

    public int size() {
        return length;
    }

    public boolean isEmpty() {
        return length == 0;
    }

    public Slice subSlice(int start, int len) {
        if (start < 0 || len < 0 || start + len > length) {
            throw new IllegalArgumentException("Invalid start or length");
        }
        return new Slice(data, offset + start, len);
    }

    @Override
    public int compareTo(Slice other) {
        if (this == other) return 0;

        int minLength = Math.min(this.length, other.length);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(this.data[this.offset + i], other.data[other.offset + i]);
            if (cmp != 0) return cmp;
        }

        return Integer.compare(this.length, other.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Slice other = (Slice) obj;
        if (length != other.length) return false;

        for (int i = 0; i < length; i++) {
            if (data[offset + i] != other.data[other.offset + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = 0; i < length; i++) {
            result = 31 * result + data[offset + i];
        }
        return result;
    }

    @Override
    public String toString() {
        return "Slice{size=" + length + "}";
    }

    public static Slice copyOf(byte[] data) {
        return new Slice(Arrays.copyOf(data, data.length));
    }

    public static Slice wrap(byte[] data) {
        return new Slice(data);
    }
}
