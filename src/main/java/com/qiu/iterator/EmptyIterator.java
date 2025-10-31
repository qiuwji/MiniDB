// src/main/java/com/qiu/iterator/EmptyIterator.java
package com.qiu.iterator;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * 空迭代器实现
 */
public class EmptyIterator<K, V> implements SeekingIterator<K, V> {
    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public void seekToFirst() throws IOException { /* no-op */ }

    @Override
    public void seek(K key) throws IOException { /* no-op */ }

    @Override
    public void next() throws IOException {
        throw new NoSuchElementException("Empty iterator has no elements");
    }

    @Override
    public K key() {
        throw new IllegalStateException("Empty iterator is not valid");
    }

    @Override
    public V value() {
        throw new IllegalStateException("Empty iterator is not valid");
    }

    @Override
    public void close() throws IOException { /* no-op */ }

    @Override
    public String toString() {
        return "EmptyIterator{}";
    }
}
