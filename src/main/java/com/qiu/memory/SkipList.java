package com.qiu.memory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 内存安全：所有返回的数据都是防御性拷贝
 * 线程安全：使用读写锁保证并发安全
 */
public class SkipList<K, V> {
    private static final int MAX_LEVEL = 12;
    private static final double P = 0.5;

    private final Node<K, V> head;
    private final Comparator<K> comparator;
    private final Random random;
    private final AtomicLong size;
    private final AtomicLong modCount;
    private int level;
    private final ReentrantReadWriteLock readWriteLock;

    private static class Node<K, V> {
        final K key;
        final V value;
        final Node<K, V>[] forward;

        @SuppressWarnings("unchecked")
        Node(K key, V value, int level) {
            this.key = key;
            this.value = value;
            this.forward = new Node[level + 1];
        }
    }

    public SkipList(Comparator<K> comparator) {
        this.comparator = Objects.requireNonNull(comparator, "Comparator cannot be null");
        this.head = new Node<>(null, null, MAX_LEVEL);
        this.level = 0;
        this.random = new Random();
        this.size = new AtomicLong(0);
        this.modCount = new AtomicLong(0);
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public void put(K key, V value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // 获取写锁
        readWriteLock.writeLock().lock();
        try {
            @SuppressWarnings("unchecked")
            Node<K, V>[] update = new Node[MAX_LEVEL + 1];
            Node<K, V> current = head;

            // 从最高层开始查找插入位置
            for (int i = level; i >= 0; i--) {
                while (current.forward[i] != null &&
                        comparator.compare(current.forward[i].key, key) < 0) {
                    current = current.forward[i];
                }
                update[i] = current;
            }

            current = current.forward[0];

            // 如果键已存在，更新值（当前实现选择插入新节点，保留历史）
            if (current != null && comparator.compare(current.key, key) == 0) {
                // 不在此处替换旧节点（MemTable使用InternalKey区别不同版本）
            }

            // 生成新节点的层级
            int newLevel = randomLevel();
            if (newLevel > level) {
                for (int i = level + 1; i <= newLevel; i++) {
                    update[i] = head;
                }
                level = newLevel;
            }

            // 创建新节点
            Node<K, V> newNode = new Node<>(key, value, newLevel);

            // 更新指针
            for (int i = 0; i <= newLevel; i++) {
                newNode.forward[i] = update[i].forward[i];
                update[i].forward[i] = newNode;
            }

            size.incrementAndGet();
            modCount.incrementAndGet();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public V get(K key) {
        Objects.requireNonNull(key, "Key cannot be null");

        // 获取读锁
        readWriteLock.readLock().lock();
        try {
            Node<K, V> current = head;

            // 从最高层开始查找
            for (int i = level; i >= 0; i--) {
                while (current.forward[i] != null &&
                        comparator.compare(current.forward[i].key, key) < 0) {
                    current = current.forward[i];
                }
            }

            current = current.forward[0];

            if (current != null && comparator.compare(current.key, key) == 0) {
                // 返回值的防御性拷贝（如果值是字节数组）
                return defensiveCopy(current.value);
            }

            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean contains(K key) {
        // 获取读锁
        readWriteLock.readLock().lock();
        try {
            return getWithoutLock(key) != null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    // 内部使用的无锁get方法，仅在持有读锁时调用
    private V getWithoutLock(K key) {
        Node<K, V> current = head;

        for (int i = level; i >= 0; i--) {
            while (current.forward[i] != null &&
                    comparator.compare(current.forward[i].key, key) < 0) {
                current = current.forward[i];
            }
        }

        current = current.forward[0];

        if (current != null && comparator.compare(current.key, key) == 0) {
            return defensiveCopy(current.value);
        }

        return null;
    }

    public boolean delete(K key) {
        Objects.requireNonNull(key, "Key cannot be null");

        // 获取写锁
        readWriteLock.writeLock().lock();
        try {
            @SuppressWarnings("unchecked")
            Node<K, V>[] update = new Node[MAX_LEVEL + 1];
            Node<K, V> current = head;
            boolean found = false;

            // 查找要删除的节点
            for (int i = level; i >= 0; i--) {
                while (current.forward[i] != null &&
                        comparator.compare(current.forward[i].key, key) < 0) {
                    current = current.forward[i];
                }
                update[i] = current;
            }

            current = current.forward[0];

            if (current != null && comparator.compare(current.key, key) == 0) {
                found = true;

                // 更新所有层的指针
                for (int i = 0; i <= level; i++) {
                    if (update[i].forward[i] != current) {
                        break;
                    }
                    update[i].forward[i] = current.forward[i];
                }

                // 降低层级如果必要
                while (level > 0 && head.forward[level] == null) {
                    level--;
                }

                size.decrementAndGet();
                modCount.incrementAndGet();
            }

            return found;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public long size() {
        // 获取读锁
        readWriteLock.readLock().lock();
        try {
            return size.get();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        // 获取读锁
        readWriteLock.readLock().lock();
        try {
            return size.get() == 0;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void clear() {
        // 获取写锁
        readWriteLock.writeLock().lock();
        try {
            for (int i = 0; i <= level; i++) {
                head.forward[i] = null;
            }
            level = 0;
            size.set(0);
            modCount.incrementAndGet();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 返回标准的 Java Iterator，元素类型为 SkipList.Entry<K,V>
     * 注意：迭代器需要持有读锁以保证遍历过程中的一致性
     */
    public Iterator<Entry<K, V>> iterator() {
        return new SkipListIterator();
    }

    private int randomLevel() {
        int level = 0;
        while (random.nextDouble() < P && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }

    @SuppressWarnings("unchecked")
    private V defensiveCopy(V value) {
        if (value instanceof byte[]) {
            byte[] original = (byte[]) value;
            byte[] copy = new byte[original.length];
            System.arraycopy(original, 0, copy, 0, original.length);
            return (V) copy;
        }
        return value;
    }

    /**
     * 跳表迭代器（实现 java.util.Iterator<SkipList.Entry<K,V>>）
     * 迭代器在创建时获取读锁，在关闭时释放读锁
     */
    public class SkipListIterator implements Iterator<Entry<K, V>> {
        private Node<K, V> current;
        private final long expectedModCount;
        private boolean lockAcquired;

        public SkipListIterator() {
            // 在构造函数中获取读锁
            readWriteLock.readLock().lock();
            this.lockAcquired = true;
            this.current = head.forward[0];
            this.expectedModCount = modCount.get();
        }

        @Override
        public boolean hasNext() {
            if (!lockAcquired) {
                throw new IllegalStateException("Iterator is no longer valid");
            }
            checkForComodification();
            return current != null;
        }

        @Override
        public Entry<K, V> next() {
            if (!lockAcquired) {
                throw new IllegalStateException("Iterator is no longer valid");
            }
            checkForComodification();
            if (current == null) {
                throw new NoSuchElementException();
            }

            Node<K, V> node = current;
            current = current.forward[0];

            // 返回防御性拷贝
            K keyCopy = defensiveCopyKey(node.key);
            V valueCopy = defensiveCopy(node.value);

            return new Entry<>(keyCopy, valueCopy);
        }

        /**
         * 释放迭代器持有的读锁
         * 建议在使用完迭代器后调用此方法，或者使用try-with-resources模式
         */
        public void close() {
            if (lockAcquired) {
                readWriteLock.readLock().unlock();
                lockAcquired = false;
            }
        }

        @SuppressWarnings("unchecked")
        private K defensiveCopyKey(K key) {
            if (key instanceof byte[]) {
                byte[] original = (byte[]) key;
                byte[] copy = new byte[original.length];
                System.arraycopy(original, 0, copy, 0, original.length);
                return (K) copy;
            }
            return key;
        }

        private void checkForComodification() {
            if (modCount.get() != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }

        // 防止忘记释放锁，添加finalize方法作为最后保障
        @Override
        protected void finalize() throws Throwable {
            if (lockAcquired) {
                readWriteLock.readLock().unlock();
                lockAcquired = false;
            }
            super.finalize();
        }
    }

    /**
     * 键值对条目
     */
    public static class Entry<K, V> {
        private final K key;
        private final V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry<?, ?> entry = (Entry<?, ?>) o;
            return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /**
     * 获取读写锁（用于外部监控或高级操作）
     */
    public ReentrantReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    /**
     * 使用try-with-resources模式的迭代方法
     */
    public void forEach(java.util.function.Consumer<Entry<K, V>> action) {
        SkipListIterator iterator = new SkipListIterator();
        try {
            while (iterator.hasNext()) {
                action.accept(iterator.next());
            }
        } finally {
            iterator.close();
        }
    }
}