// 文件：SkipList.java
package com.qiu.memory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 线程不安全的跳表实现
 * 内存安全：所有返回的数据都是防御性拷贝
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
    }

    public void put(K key, V value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

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
    }

    public V get(K key) {
        Objects.requireNonNull(key, "Key cannot be null");

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
    }

    public boolean contains(K key) {
        return get(key) != null;
    }

    public boolean delete(K key) {
        Objects.requireNonNull(key, "Key cannot be null");

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
    }

    public long size() {
        return size.get();
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

    public void clear() {
        for (int i = 0; i <= level; i++) {
            head.forward[i] = null;
        }
        level = 0;
        size.set(0);
        modCount.incrementAndGet();
    }

    /**
     * 返回标准的 Java Iterator，元素类型为 SkipList.Entry<K,V>
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
     */
    public class SkipListIterator implements Iterator<Entry<K, V>> {
        private Node<K, V> current;
        private final long expectedModCount;

        public SkipListIterator() {
            this.current = head.forward[0];
            this.expectedModCount = modCount.get();
        }

        @Override
        public boolean hasNext() {
            checkForComodification();
            return current != null;
        }

        @Override
        public Entry<K, V> next() {
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
    }
}
