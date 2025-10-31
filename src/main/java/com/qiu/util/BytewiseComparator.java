package com.qiu.util;

import java.util.Comparator;

/**
 * 字节数组比较器
 */
public class BytewiseComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] a, byte[] b) {
        if (a == b) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }
}
