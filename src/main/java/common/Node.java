package common;

/**
 * Created by hms on 2016/12/12.
 */
import java.io.File;
import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class Node implements Serializable {

    /** 是否为叶子节点 */
    protected boolean isLeaf;

    /** 是否为根节点*/
    protected boolean isRoot;

    /** 父节点文件名 */
    protected String parentFileName;

    /** 父节点 */
    protected Node parent;

    protected String previousFileName;
    /** 叶节点的前节点*/
    protected Node previous;

    protected String nextFileName;
    /** 叶节点的后节点*/
    protected Node next;

    /** 节点的关键字 */
    protected List<Entry<Comparable, Object>> entries;

    protected List<String> childrenFileNames;
    /** 子节点 */
    protected List<Node> children;

    /** 该节点的文件名 */
    protected String fileName;

    public Node(boolean isLeaf) {
        this.isLeaf = isLeaf;
        entries = new ArrayList<Entry<Comparable, Object>>();

        if (!isLeaf) {
            children = new ArrayList<Node>();
        }

        this.fileName = "node_" + System.currentTimeMillis();
    }

    public Node(boolean isLeaf, boolean isRoot) {
        this(isLeaf);
        this.isRoot = isRoot;

        this.fileName = "root_" + System.currentTimeMillis();
    }

    public Object get(Comparable key) {  //TODO

        //如果是叶子节点
        if (isLeaf) {
            for (Entry<Comparable, Object> entry : entries) {
                if (entry.getKey().compareTo(key) == 0) {
                    //返回找到的对象
                    return entry.getValue();
                }
            }
            //未找到所要查询的对象
            return null;

            //如果不是叶子节点
        }else {
            //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                return children.get(0).get(key);
                //如果key大于节点最右边的key，沿最后一个子节点继续搜索
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                return children.get(children.size()-1).get(key);
                //否则沿比key大的前一个子节点继续搜索
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        return children.get(i).get(key);
                    }
                }
            }
        }

        return null;
    }

    public void insertOrUpdate(Comparable key, Object obj, BplusTree tree){
        //如果是叶子节点
        if (isLeaf){
            //不需要分裂，直接插入或更新
            if (contains(key) || entries.size() < tree.getOrder()){
                insertOrUpdate(key, obj);
                if (parent != null) { //TODO 序列化后还能保持Node信息吗
                    //更新父节点
                    parent.updateInsert(tree);
                }

                //需要分裂
            }else {
                //分裂成左右两个节点
                Node left = new Node(true);
                Node right = new Node(true);
                //设置链接
                if (previous != null){
                    previous.setNext(left);
                    left.setPrevious(previous);
                }
                if (next != null) {
                    next.setPrevious(right);
                    right.setNext(next);
                }
                if (previous == null){
                    tree.setHead(left);
                }

                left.setNext(right);
                right.setPrevious(left);
                previous = null;
                next = null;

                //左右两个节点关键字长度
                int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
                int rightSize = (tree.getOrder() + 1) / 2;
                //复制原节点关键字到分裂出来的新节点
                insertOrUpdate(key, obj);
                for (int i = 0; i < leftSize; i++){
                    left.getEntries().add(entries.get(i));
                }
                for (int i = 0; i < rightSize; i++){
                    right.getEntries().add(entries.get(leftSize + i));
                }

                //如果不是根节点
                if (parent != null) {
                    //调整父子节点关系
                    int index = parent.getChildren().indexOf(this);
                    parent.getChildren().remove(this);
                    left.setParent(parent);
                    right.setParent(parent);
                    parent.getChildren().add(index,left);
                    parent.getChildren().add(index + 1, right);
                    setEntries(null);
                    setChildren(null);

                    //父节点插入或更新关键字
                    parent.updateInsert(tree);
                    setParent(null);
                    //如果是根节点
                }else {
                    isRoot = false;
                    Node parent = new Node(false, true);
                    tree.setRoot(parent);
                    left.setParent(parent);
                    right.setParent(parent);
                    parent.getChildren().add(left);
                    parent.getChildren().add(right);
                    setEntries(null);
                    setChildren(null);

                    //更新根节点
                    parent.updateInsert(tree);
                }


            }

            //如果不是叶子节点
        }else {
            //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                children.get(0).insertOrUpdate(key, obj, tree);
                //如果key大于节点最右边的key，沿最后一个子节点继续搜索
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                children.get(children.size()-1).insertOrUpdate(key, obj, tree);
                //否则沿比key大的前一个子节点继续搜索
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        children.get(i).insertOrUpdate(key, obj, tree);
                        break;
                    }
                }
            }
        }
    }

    /** 插入节点后中间节点的更新 */
    protected void updateInsert(BplusTree tree){

        validate(this, tree);

        //如果子节点数超出阶数，则需要分裂该节点
        if (children.size() > tree.getOrder()) {
            //分裂成左右两个节点
            Node left = new Node(false);
            Node right = new Node(false);
            //左右两个节点关键字长度
            int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
            int rightSize = (tree.getOrder() + 1) / 2;
            //复制子节点到分裂出来的新节点，并更新关键字
            for (int i = 0; i < leftSize; i++){
                left.getChildren().add(children.get(i));
                left.getEntries().add(new SimpleEntry(children.get(i).getEntries().get(0).getKey(), null));
                children.get(i).setParent(left);
            }
            for (int i = 0; i < rightSize; i++){
                right.getChildren().add(children.get(leftSize + i));
                right.getEntries().add(new SimpleEntry(children.get(leftSize + i).getEntries().get(0).getKey(), null));
                children.get(leftSize + i).setParent(right);
            }

            //如果不是根节点
            if (parent != null) {
                //调整父子节点关系
                int index = parent.getChildren().indexOf(this);
                parent.getChildren().remove(this);
                left.setParent(parent);
                right.setParent(parent);
                parent.getChildren().add(index,left);
                parent.getChildren().add(index + 1, right);
                setEntries(null);
                setChildren(null);

                //父节点更新关键字
                parent.updateInsert(tree);
                setParent(null);
                //如果是根节点
            }else {
                isRoot = false;
                Node parent = new Node(false, true);
                tree.setRoot(parent);
                left.setParent(parent);
                right.setParent(parent);
                parent.getChildren().add(left);
                parent.getChildren().add(right);
                setEntries(null);
                setChildren(null);

                //更新根节点
                parent.updateInsert(tree);
            }
        }
    }

    /** 调整节点关键字*/
    protected static void validate(Node node, BplusTree tree) {

        // 如果关键字个数与子节点个数相同
        if (node.getEntries().size() == node.getChildren().size()) {
            for (int i = 0; i < node.getEntries().size(); i++) {
                Comparable key = node.getChildren().get(i).getEntries().get(0).getKey();
                if (node.getEntries().get(i).getKey().compareTo(key) != 0) {
                    node.getEntries().remove(i);
                    node.getEntries().add(i, new SimpleEntry(key, null));
                    if(!node.isRoot()){
                        validate(node.getParent(), tree);
                    }
                }
            }
            // 如果子节点数不等于关键字个数但仍大于M / 2并且小于M，并且大于2
        } else if (node.isRoot() && node.getChildren().size() >= 2
                ||node.getChildren().size() >= tree.getOrder() / 2
                && node.getChildren().size() <= tree.getOrder()
                && node.getChildren().size() >= 2) {
            node.getEntries().clear();
            for (int i = 0; i < node.getChildren().size(); i++) {
                Comparable key = node.getChildren().get(i).getEntries().get(0).getKey();
                node.getEntries().add(new SimpleEntry(key, null));
                if (!node.isRoot()) {
                    validate(node.getParent(), tree);
                }
            }
        }
    }

    /** 判断当前节点是否包含该关键字*/
    protected boolean contains(Comparable key) {
        for (Entry<Comparable, Object> entry : entries) {
            if (entry.getKey().compareTo(key) == 0) {
                return true;
            }
        }
        return false;
    }

    /** 插入到当前节点的关键字中*/
    protected void insertOrUpdate(Comparable key, Object obj){
        Entry<Comparable, Object> entry = new SimpleEntry<Comparable, Object>(key, obj);
        //如果关键字列表长度为0，则直接插入
        if (entries.size() == 0) {
            entries.add(entry);
            return;
        }
        //否则遍历列表
        for (int i = 0; i < entries.size(); i++) {
            //如果该关键字键值已存在，则更新
            if (entries.get(i).getKey().compareTo(key) == 0) {
                entries.get(i).setValue(obj);
                return;
                //否则插入
            }else if (entries.get(i).getKey().compareTo(key) > 0){
                //插入到链首
                if (i == 0) {
                    entries.add(0, entry);
                    return;
                    //插入到中间
                }else {
                    entries.add(i, entry);
                    return;
                }
            }
        }
        //插入到末尾
        entries.add(entries.size(), entry);
    }

    /** 删除节点*/
    protected void remove(Comparable key){
        int index = -1;
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).getKey().compareTo(key) == 0) {
                index = i;
                break;
            }
        }
        if (index != -1) {
            entries.remove(index);
        }
    }

    public Node getPrevious() {
        return previous;
    }

    public void setPrevious(Node previous) {
        this.previous = previous;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public List<Entry<Comparable, Object>> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry<Comparable, Object>> entries) {
        this.entries = entries;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean isRoot) {
        this.isRoot = isRoot;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFile(String fileName) {
        this.fileName = fileName;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("isRoot: ");
        sb.append(isRoot);
        sb.append(", ");
        sb.append("isLeaf: ");
        sb.append(isLeaf);
        sb.append(", ");
        sb.append("keys: ");
        for (Entry entry : entries){
            sb.append(entry.getKey());
            sb.append(", ");
        }
        sb.append(", ");
        return sb.toString();
    }

}
