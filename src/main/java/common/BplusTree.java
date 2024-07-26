package common;

/**
 * Created by hms on 2016/12/12.
 */

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class BplusTree implements B, Serializable {

    /**
     * 根节点文件名
     */
    protected String rootFile;

    /**
     * 根节点
     */
    protected Node root;
    {
        readNodeFromFile(this.rootFile);
    }

    /**
     * 叶子节点的链表头文件名
     */
    protected String headFile;

    /**
     * 叶子节点的链表头
     */
    protected Node head;
    {
        readNodeFromFile(this.headFile);
    }

    /**
     * 阶数，M值
     */
    protected int order;

    /**
     * 存放读取过的节点 类似缓存
     */
    protected ArrayList<Node> Nodes = new ArrayList<>();

    public Node getHead() {
        return head;
    }

    public void setHead(Node head) {
        this.head = head;
        this.headFile = head.getFileName();
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
        this.rootFile = root.getFileName();
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Override
    public Object get(Comparable key) {
        return root.get(key);
    }

    @Override
    public void insertOrUpdate(Comparable key, Object obj) {
        root.insertOrUpdate(key, obj, this);
    }

    public BplusTree(int order) {
        if (order < 3) {
            System.out.print("order must be greater than 2");
            System.exit(0);
        }
        this.order = order;
        root = new Node(true, true); // TODO
        head = root;
    }

    public Node readNodeFromFile(String filePath) {
        Node node = null;
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(filePath))) {
            // 使用 readObject 方法读取序列化的对象
            node = (Node) objectInputStream.readObject();
        } catch (EOFException e) {
            // 表示文件已经结束，没有更多数据可读
            System.out.println("已到达文件末尾，没有读取到对象");
        } catch (IOException | ClassNotFoundException e) {
            // 处理其他可能发生的异常
            e.printStackTrace();
        }
        return node;
    }

    //测试
    public static void main(String[] args) throws IOException {
        BplusTree tree = new BplusTree(6); //  TODO 如果有Tree文件直接读
        Random random = new Random();
        long current = System.currentTimeMillis();
        for (int j = 0; j < 100000; j++) {
            for (int i = 0; i < 100; i++) {
                int randomNumber = random.nextInt(1000);
                tree.insertOrUpdate(randomNumber, randomNumber);
            }
        }
        long duration = System.currentTimeMillis() - current;
        System.out.println("time elpsed for duration: " + duration);
        int search = 80;
        System.out.print(tree.get(search));
        Node next = tree.getHead();
        int count = 0;
        while (true) {
            if (next == null) break;
            ++count;
            List<Entry<Comparable, Object>> entries = next.getEntries();
            File file = new File(String.valueOf(count) + ".txt");
            next.setFile(file);
            ObjectOutputStream objectOutputStream =
                    new ObjectOutputStream(new FileOutputStream(file));
            objectOutputStream.writeObject(next);
            objectOutputStream.close();
            next = next.getNext();
        }
        File treeFile = new File("BplusTree.txt");
        ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(new FileOutputStream(treeFile));
        objectOutputStream.writeObject(tree);
        objectOutputStream.close();
    }
}
