// src/main/java/com/qiu/iterator/IteratorUtils.java
package com.qiu.iterator;

import java.io.IOException;
import java.util.*;

/**
 * 迭代器工具类
 */
public class IteratorUtils {
    private IteratorUtils() {}

    /**
     * 将普通迭代器转换为支持定位的迭代器（简单封装：只支持 seekToFirst）
     */
    public static <K, V> SeekingIterator<K, V> wrapIterator(Iterator<K> keyIterator, Iterator<V> valueIterator) {
        return new WrappedIterator<>(keyIterator, valueIterator);
    }

    /**
     * 创建单元素迭代器
     */
    public static <K, V> SeekingIterator<K, V> singletonIterator(K key, V value) {
        return new SingletonIterator<>(key, value);
    }

    /**
     * 合并多个迭代器（需要提供比较器）
     */
    @SafeVarargs
    public static <K, V> SeekingIterator<K, V> mergeIterators(Comparator<K> comparator, SeekingIterator<K, V>... iterators) {
        List<SeekingIterator<K, V>> list = new ArrayList<>();
        if (iterators != null) {
            for (SeekingIterator<K, V> it : iterators) {
                if (it != null) list.add(it);
            }
        }
        return new MergeIterator<>(list, comparator);
    }

    /**
     * 将迭代器中的所有元素收集为列表（会关闭迭代器）
     */
    public static <K, V> List<KeyValue<K, V>> toList(SeekingIterator<K, V> iterator) throws IOException {
        List<KeyValue<K, V>> result = new ArrayList<>();
        try {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                result.add(new KeyValue<>(iterator.key(), iterator.value()));
                iterator.next();
            }
        } finally {
            iterator.close();
        }
        return result;
    }

    public static class KeyValue<K, V> {
        private final K key;
        private final V value;

        public KeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() { return key; }
        public V getValue() { return value; }

        @Override
        public String toString() {
            return String.format("KeyValue{key=%s, value=%s}", key, value);
        }
    }

    /* WrappedIterator & SingletonIterator (simple implementations) */

    private static class WrappedIterator<K, V> implements SeekingIterator<K, V> {
        private final Iterator<K> keyIterator;
        private final Iterator<V> valueIterator;
        private K currentKey;
        private V currentValue;
        private boolean valid;

        WrappedIterator(Iterator<K> keyIterator, Iterator<V> valueIterator) {
            this.keyIterator = Objects.requireNonNull(keyIterator);
            this.valueIterator = Objects.requireNonNull(valueIterator);
            this.valid = false;
        }

        @Override
        public boolean isValid() { return valid; }

        @Override
        public void seekToFirst() { valid = keyIterator.hasNext() && valueIterator.hasNext();
            if (valid) { currentKey = keyIterator.next(); currentValue = valueIterator.next(); } }

        @Override
        public void seek(K key) { // 简单实现：从头线性查找
            seekToFirst();
            while (valid && !Objects.equals(currentKey, key)) {
                try { next(); } catch (IOException e) { throw new RuntimeException(e); }
            }
        }

        @Override
        public void next() throws IOException {
            if (!valid) throw new NoSuchElementException();
            valid = keyIterator.hasNext() && valueIterator.hasNext();
            if (valid) { currentKey = keyIterator.next(); currentValue = valueIterator.next(); }
        }

        @Override
        public K key() { if (!valid) throw new IllegalStateException(); return currentKey; }

        @Override
        public V value() { if (!valid) throw new IllegalStateException(); return currentValue; }

        @Override
        public void close() { /* no-op */ }
    }

    private static class SingletonIterator<K, V> implements SeekingIterator<K, V> {
        private final K key;
        private final V value;
        private boolean consumed;

        SingletonIterator(K key, V value) { this.key = key; this.value = value; this.consumed = false; }

        @Override public boolean isValid() { return !consumed; }
        @Override public void seekToFirst() { consumed = false; }
        @Override public void seek(K key) { consumed = !Objects.equals(this.key, key); }
        @Override public void next() throws IOException { if (!isValid()) throw new NoSuchElementException(); consumed = true; }
        @Override public K key() { if (!isValid()) throw new IllegalStateException(); return key; }
        @Override public V value() { if (!isValid()) throw new IllegalStateException(); return value; }
        @Override public void close() throws IOException { /* no-op */ }
    }
}
