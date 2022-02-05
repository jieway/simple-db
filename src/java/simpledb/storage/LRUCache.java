package simpledb.storage;

import java.util.*;
import java.util.stream.Collectors;


public class LRUCache<K, V> {

    public class Node {
        public K    key;
        public V    val;
        public Node next;
        public Node prev;

        public Node(K key, V val) {
            this.key = key;
            this.val = val;
        }
    }

    private final int          maxSize;
    private final Map<K, Node> nodeMap;
    private final Node         head;
    private final Node         tail;

    public LRUCache(int maxSize) {
        this.maxSize    = maxSize;
        this.head       = new Node(null , null);
        this.tail       = new Node(null , null);
        this.head.next  = tail;
        this.tail.prev  = head;
        this.nodeMap    = new HashMap<>();
    }

    // 在头部插入
    public void linkToHead(Node node) {
        node.next = this.head.next;
        this.head.next.prev = node;
        this.head.next = node;
        node.prev = this.head;
    }

    // 删除中间节点
    public void removeNode(Node node) {
        if (node.prev != null && node.next != null) {
            node.next.prev = node.prev;
            node.prev.next = node.next;
        }
    }

    // 移除最后一个节点
    public Node removeLast() {
        Node last = this.tail.prev;
        removeNode(last);
        return last;
    }

    // 根据 key 移除 Node
    public void remove(K key) {
        if (this.nodeMap.containsKey(key)) {
            removeNode(this.nodeMap.get(key));
            this.nodeMap.remove(key);
        }
    }

    public void moveToHead(Node node) {
        removeNode(node);
        linkToHead(node);
    }

    public V get(K key) {
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            moveToHead(node);
            return node.val;
        }
        return null;
    }

    public V put(K key, V val) {
        Node newNode = new Node(key, val);
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            node.val = val;
            moveToHead(node);
        }else {
            // 空间不够了，删除 LRU 并更新 nodeMap
            if (this.maxSize == this.nodeMap.size()) {
                Node node = removeLast();
                this.nodeMap.remove(node.key);
                return node.val;
            }
            // 此时能保证空间足够，插入并更新频率
            this.nodeMap.put(key , newNode);
            linkToHead(newNode);
        }
        return null;
    }

    public Iterator<V> valueIterator() {
        final Collection<Node> nodes = this.nodeMap.values();
        final List<V> valueList = nodes.stream().map(x -> x.val).collect(Collectors.toList());
        return valueList.iterator();
    }

}