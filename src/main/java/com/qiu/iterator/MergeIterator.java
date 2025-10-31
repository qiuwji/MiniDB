// src/main/java/com/qiu/iterator/MergeIterator.java
package com.qiu.iterator;

import java.io.IOException;
import java.util.*;

/**
 * 合并多个迭代器，按键顺序返回数据（去除重复键）
 */
public class MergeIterator<K, V> implements SeekingIterator<K, V> {
    private final PriorityQueue<IteratorWrapper<K, V>> heap;
    private final List<IteratorWrapper<K, V>> wrappers; // 用于重置/seek
    private final Comparator<K> comparator;
    private IteratorWrapper<K, V> current;
    private boolean valid;

    public MergeIterator(List<SeekingIterator<K, V>> iterators, Comparator<K> comparator) {
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");
        this.wrappers = new ArrayList<>();
        if (iterators != null) {
            for (SeekingIterator<K, V> it : iterators) {
                if (it != null) {
                    wrappers.add(new IteratorWrapper<>(it));
                }
            }
        }

        // 使用仅 Comparator 的构造器，避免 initialCapacity 为 0 导致异常
        this.heap = new PriorityQueue<>((a, b) -> {
            K ka = a.currentKey();
            K kb = b.currentKey();
            if (ka == null && kb == null) return 0;
            if (ka == null) return 1;
            if (kb == null) return -1;
            return comparator.compare(ka, kb);
        });

        // 将所有有效迭代器加入堆
        for (IteratorWrapper<K, V> w : wrappers) {
            if (w.isValid()) heap.offer(w);
        }

        advance();
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public void seekToFirst() throws IOException {
        heap.clear();
        for (IteratorWrapper<K, V> w : wrappers) {
            w.iterator.seekToFirst();
            w.updateCurrent();
            if (w.isValid()) heap.offer(w);
        }
        advance();
    }

    @Override
    public void seek(K key) throws IOException {
        Objects.requireNonNull(key, "Key cannot be null");
        heap.clear();
        for (IteratorWrapper<K, V> w : wrappers) {
            w.iterator.seek(key);
            w.updateCurrent();
            if (w.isValid()) heap.offer(w);
        }
        advance();
    }

    @Override
    public void next() throws IOException {
        if (!valid) throw new NoSuchElementException("Iterator is not valid");

        // 推进当前底层迭代器并重新加入堆（如果仍然有效）
        if (current != null) {
            current.iterator.next();
            current.updateCurrent();
            if (current.isValid()) heap.offer(current);
        }

        advance();
    }

    @Override
    public K key() {
        if (!valid) throw new IllegalStateException("Iterator is not valid");
        return current.currentKey();
    }

    @Override
    public V value() {
        if (!valid) throw new IllegalStateException("Iterator is not valid");
        return current.currentValue();
    }

    @Override
    public void close() throws IOException {
        // 关闭所有底层迭代器（去重）
        Set<SeekingIterator<K, V>> closed = new HashSet<>();
        for (IteratorWrapper<K, V> w : wrappers) {
            if (!closed.contains(w.iterator)) {
                w.iterator.close();
                closed.add(w.iterator);
            }
        }
        heap.clear();
        wrappers.clear();
        current = null;
        valid = false;
    }

    /**
     * 推进到下一个键（并去重：保留第一次出现的键）
     */
    private void advance() {
        if (heap.isEmpty()) {
            current = null;
            valid = false;
            return;
        }

        current = heap.poll();
        valid = true;

        // 跳过堆中与 current 相同键的重复项（先把重复项推进）
        while (!heap.isEmpty() && comparator.compare(current.currentKey(), heap.peek().currentKey()) == 0) {
            IteratorWrapper<K, V> dup = heap.poll();
            try {
                dup.iterator.next();
                dup.updateCurrent();
                if (dup.isValid()) heap.offer(dup);
            } catch (IOException e) {
                // 如果推进失败，忽略该迭代器
                System.err.println("Failed to advance duplicate iterator: " + e.getMessage());
            }
        }
    }

    /**
     * 获取底层活跃迭代器数量（堆中 + 当前）
     */
    public int getIteratorCount() {
        return heap.size() + (current != null ? 1 : 0);
    }

    /**
     * 包装底层 SeekingIterator，用于缓存当前键值
     */
    private static class IteratorWrapper<K, V> {
        final SeekingIterator<K, V> iterator;
        private K currentKey;
        private V currentValue;

        IteratorWrapper(SeekingIterator<K, V> iterator) {
            this.iterator = iterator;
            updateCurrent();
        }

        void updateCurrent() {
            if (iterator.isValid()) {
                this.currentKey = iterator.key();
                this.currentValue = iterator.value();
            } else {
                this.currentKey = null;
                this.currentValue = null;
            }
        }

        boolean isValid() {
            return currentKey != null;
        }

        K currentKey() { return currentKey; }
        V currentValue() { return currentValue; }
    }
}
