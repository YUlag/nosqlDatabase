package common;

/**
 * Created by hms on 2016/12/12.
 */

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class BplusTree implements B, Serializable {
    String indexDir = "index"+ File.separator;
    /**
     * 根节点文件名
     */
    protected String rootFile;

    /**
     * 根节点
     */
    protected Node root;

    /**
     * 叶子节点的链表头文件名
     */
    protected String headFile;

    /**
     * 叶子节点的链表头
     */
    protected Node head;

    /**
     * 阶数，M值
     */
    protected int order;

    /**
     * 存放读取过的节点 类似缓存
     */
    protected HashMap<String, Node> nodes = new HashMap<>();

    protected ArrayList<String> deletedFiles = new ArrayList<>();

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
        return root.get(key, this);
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
        root = new Node(true, true);
        nodes.put(root.getFileName(), root);

        head = root;
    }

    public BplusTree() throws IOException, ClassNotFoundException {
        File treeFile = new File(indexDir + "BplusTree.txt");
        BplusTree tree = null;

        if (treeFile.exists()) {
            tree = getTreeFromFile(treeFile);
        } else {
            tree = new BplusTree(100);
        }

        root = tree.root;
        rootFile = root.getFileName();
        head = tree.head;
        headFile = head.getFileName();
        order = tree.order;
        nodes.put(root.getFileName(), root);
    }

    public Node readNodeFromFile(String filePath,String indexDir) {
        Node node = null;
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(indexDir + filePath))) {
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

    public void writeNodeToFile(Node node,String indexDir) {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(indexDir + node.fileName))) {
            // 使用 writeObject 方法写入对象
            objectOutputStream.writeObject(node);
        } catch (IOException e) {
            // 处理其他可能发生的异常
            e.printStackTrace();
        }
    }

    public void wirteTreeToFile(BplusTree tree) throws IOException {
        File treeFile = new File(indexDir + "BplusTree.txt");
        ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(new FileOutputStream(treeFile));
        objectOutputStream.writeObject(tree);
        objectOutputStream.close();

    }

    public static BplusTree getTreeFromFile(File treeFile) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream =
                new ObjectInputStream(new FileInputStream(treeFile));
        BplusTree tree = (BplusTree) objectInputStream.readObject();
        objectInputStream.close();
        tree.nodes.put(tree.rootFile, tree.root);

        return tree;
    }

    public void deleteNodeFromFile(String filePath) {
        File file = new File(indexDir + filePath);
        if (file.exists()) {
            file.delete();
        }
    }

    public void save(BplusTree tree) throws IOException {
        for (String filePath : deletedFiles) {
            deleteNodeFromFile(filePath);
        }
        deletedFiles.clear();

        for (Node node : nodes.values()) {
            writeNodeToFile(node,indexDir);
        }
        nodes.clear();
        nodes.put(tree.rootFile, tree.root);

        wirteTreeToFile(tree);
    }

    public void clear() {
        File folder = new File(indexDir);
        // 检查文件夹是否存在
        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        file.delete(); // 删除文件
                    }
                }
            }
        }

        root = new Node(true, true);
        rootFile = root.getFileName();
        nodes.clear();
        deletedFiles.clear();

        nodes.put(rootFile, root);
        head = root;
        headFile = head.getFileName();
    }
    //测试
//    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        Random random = new Random();
//
//        long current = System.currentTimeMillis();
//
//        for (int i = 11000; i < 22000; i++) {
//            tree.insertOrUpdate(i, i+1);
//        }
//
//        long duration = System.currentTimeMillis() - current;
//        System.out.println("time elpsed for duration: " + duration);
//
//        int search = 9999;
//        System.out.print(tree.get(search));
//
//        tree.save(tree,indexDir);
//    }
}
