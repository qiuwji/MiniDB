package com.qiu.iterator;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * SSTable迭代器的适配器，实现SeekingIterator接口
 */
public class TableIterator implements SeekingIterator<byte[], byte[]> {
    private final com.qiu.sstable.SSTable.TableIterator delegate;
    private boolean closed;

    public TableIterator(com.qiu.sstable.SSTable.TableIterator delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate iterator cannot be null");
        this.closed = false;
    }

    @Override
    public boolean isValid() {
        checkNotClosed();
        return delegate.isValid();
    }

    @Override
    public void seekToFirst() throws IOException {
        checkNotClosed();
        delegate.seekToFirst();
    }

    @Override
    public void seek(byte[] key) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "Key cannot be null");
        delegate.seek(key);
    }

    @Override
    public void next() throws IOException {
        checkNotClosed();
        if (!isValid()) {
            throw new NoSuchElementException("Iterator is not valid");
        }
        delegate.next();
    }

    @Override
    public byte[] key() {
        checkNotClosed();
        if (!isValid()) {
            throw new IllegalStateException("Iterator is not valid");
        }
        return delegate.key();
    }

    @Override
    public byte[] value() {
        checkNotClosed();
        if (!isValid()) {
            throw new IllegalStateException("Iterator is not valid");
        }
        return delegate.value();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            // TableIterator没有close方法，这里只是标记为关闭
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("TableIterator is closed");
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return String.format("TableIterator{valid=%s, closed=%s}",
                delegate.isValid(), closed);
    }
}