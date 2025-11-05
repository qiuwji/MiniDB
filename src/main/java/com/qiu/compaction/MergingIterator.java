// src/main/java/com/qiu/compaction/MergingIterator.java
package com.qiu.compaction;

import com.qiu.util.BytewiseComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 合并多个表迭代器，按键顺序返回数据（去重，保留最新版本）
 */
public class MergingIterator {
    private final PriorityQueue<IteratorWrapper> heap;
    private final java.util.Comparator<byte[]> comparator;
    private final List<IteratorWrapper> wrappers; // 底层包装迭代器列表（用于 seek/重置）
    private IteratorWrapper current;

    /**
     * 构造器：接受已包装的 TableIterator 列表
     */
    public MergingIterator(List<TableIterator> iterators) {
        this.comparator = new BytewiseComparator();
        this.wrappers = new ArrayList<>();

        // 先创建包装器列表
        if (iterators != null) {
            for (TableIterator t : iterators) {
                wrappers.add(new IteratorWrapper(t));
            }
        }

        // <-- 修改点 1: 修复比较器 (Comparator) -->
        this.heap = new PriorityQueue<>( (a, b) -> {
            // 可能发生 currentKey 为 null 的情况，做空检查以保证 Comparator 稳定
            byte[] ka = a.currentKey();
            byte[] kb = b.currentKey();
            if (ka == null && kb == null) return 0;
            if (ka == null) return 1;
            if (kb == null) return -1;

            // 1. 比较 Key
            int keyCmp = comparator.compare(ka, kb);
            if (keyCmp != 0) {
                return keyCmp;
            }

            // 2. Key 相同，比较 FileNumber (DESCENDING - 倒序)
            // fileNumber 越大 = 越新 = 优先级越高（值越小）
            return Long.compare(b.getFileNumber(), a.getFileNumber());
        });
        // <-- 结束修改点 1 -->


        // 将所有有效迭代器加入堆
        for (IteratorWrapper w : wrappers) {
            if (w.isValid()) {
                heap.offer(w);
            }
        }

        advance();
    }

    public boolean isValid() {
        return current != null;
    }

    public byte[] key() {
        if (!isValid()) throw new IllegalStateException("Iterator is not valid");
        return current.currentKey();
    }

    public byte[] value() {
        if (!isValid()) throw new IllegalStateException("Iterator is not valid");
        return current.currentValue();
    }

    public void next() {
        if (!isValid()) return;

        if (current.isValid()) {
            current.iter.next();
            if (current.isValid()) {
                current.updateCurrent();
                heap.offer(current);
            }
        }
        advance();
    }

    public void seekToFirst() {
        // 重置所有底层迭代器到头部并重建堆
        heap.clear();
        for (IteratorWrapper w : wrappers) {
            w.iter.seekToFirst();
            w.updateCurrent();
            if (w.isValid()) heap.offer(w);
        }
        advance();
    }

    public void seek(byte[] target) {
        heap.clear();
        for (IteratorWrapper w : wrappers) {
            w.iter.seek(target);
            w.updateCurrent();
            if (w.isValid()) heap.offer(w);
        }
        advance();
    }

    /**
     * 推进到下一个不同的键（去重）
     */
    private void advance() {
        if (heap.isEmpty()) {
            current = null;
            return;
        }

        current = heap.poll();

        // 跳过与堆顶相同键的重复项（保留第一次出现，即版本最新的）
        while (!heap.isEmpty() && Arrays.equals(current.currentKey(), heap.peek().currentKey())) {
            IteratorWrapper dup = heap.poll();
            dup.iter.next();
            if (dup.isValid()) {
                dup.updateCurrent();
                heap.offer(dup);
            }
        }
    }

    /**
     * 包装底层表迭代器
     */
    // <-- 修改点 2: IteratorWrapper -->
    private static class IteratorWrapper {
        final TableIterator iter;
        private byte[] currentKey;
        private byte[] currentValue;
        private final long fileNumber; // 添加 fileNumber

        IteratorWrapper(TableIterator iter) {
            this.iter = iter;
            this.fileNumber = iter.getFileNumber(); // 获取 fileNumber
            updateCurrent();
        }

        void updateCurrent() {
            if (iter.isValid()) {
                this.currentKey = iter.key();
                this.currentValue = iter.value();
            } else {
                this.currentKey = null;
                this.currentValue = null;
            }
        }

        boolean isValid() {
            return currentKey != null;
        }

        byte[] currentKey() { return currentKey; }
        byte[] currentValue() { return currentValue; }
        long getFileNumber() { return fileNumber; } // 添加 getter
    }
    // <-- 结束修改点 2 -->


    /**
     * 表迭代器适配器（包装你项目中的 com.qiu.sstable.TableIterator）
     */
    // <-- 修改点 3: TableIterator -->
    public static class TableIterator {
        private final com.qiu.sstable.SSTable.TableIterator delegate;
        private final long fileNumber; // 添加 fileNumber

        public TableIterator(com.qiu.sstable.SSTable.TableIterator delegate, long fileNumber) {
            this.delegate = delegate;
            this.fileNumber = fileNumber; // 修改构造函数
        }

        public long getFileNumber() { // 添加 getter
            return fileNumber;
        }

        public boolean isValid() {
            return delegate.isValid();
        }

        public byte[] key() {
            return delegate.key();
        }

        public byte[] value() {
            return delegate.value();
        }

        public void next() {
            try {
                if (delegate.isValid()) {
                    delegate.next();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to advance underlying iterator", e);
            }
        }

        public void seekToFirst() {
            try {
                delegate.seekToFirst();
            } catch (IOException e) {
                throw new RuntimeException("Failed to seekToFirst", e);
            }
        }

        public void seek(byte[] target) {
            try {
                delegate.seek(target);
            } catch (IOException e) {
                throw new RuntimeException("Failed to seek", e);
            }
        }
    }
    // <-- 结束修改点 3 -->
}