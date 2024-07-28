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
import java.util.UUID;

public class Node implements Serializable {
    /** 是否为叶子节点 */
    protected boolean isLeaf;

    /** 是否为根节点*/
    protected boolean isRoot;

    /** 父节点文件名 */
    protected String parentFileName;

    /** 叶节点的前节点文件名 */
    protected String previousFileName;

    /** 叶节点的后节点文件名 */
    protected String nextFileName;

    /** 节点的关键字 */
    protected List<Entry<Comparable, Object>> entries;

    /** 子节点文件名 */
    protected List<String> childrenFileNames;

    /** 该节点的文件名 */
    protected String fileName;

    public Node(boolean isLeaf) {
        this.isLeaf = isLeaf;
        entries = new ArrayList<Entry<Comparable, Object>>();

        if (!isLeaf) {
            childrenFileNames = new ArrayList<String>();
        }

        this.fileName = "node_" + UUID.randomUUID();
    }

    public Node(boolean isLeaf, boolean isRoot) {
        this(isLeaf);
        this.isRoot = isRoot;
        this.fileName = "root_" + UUID.randomUUID();
    }

    public Object get(Comparable key, BplusTree tree) {

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
        }
        //如果不是叶子节点
        else {
            //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                return getNodeByFileName(childrenFileNames.get(0),tree).get(key,tree);
                //如果key大于节点最右边的key，沿最后一个子节点继续搜索
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                return getNodeByFileName(childrenFileNames.get(childrenFileNames.size()-1),tree).get(key,tree);
                //否则沿比key大的前一个子节点继续搜索
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        return getNodeByFileName(childrenFileNames.get(i),tree).get(key,tree);
                    }
                }
            }
        }
        return null;
    }

    public void insertOrUpdate(Comparable key, Object obj, BplusTree tree){
        //如果是叶子节点
        if (isLeaf){
            Node parent = getNodeByFileName(parentFileName,tree);
            //不需要分裂，直接插入或更新
            if (contains(key) || entries.size() < tree.getOrder()){
                insertOrUpdate(key, obj);
                if (parent != null) {
                    //更新父节点
                    parent.updateInsert(tree);
                }

                //需要分裂
            }else {
                Node previous = getNodeByFileName(previousFileName,tree);
                Node next = getNodeByFileName(nextFileName,tree);
                //分裂成左右两个节点
                Node left = new Node(true);
                Node right = new Node(true);

                tree.nodes.put(left.fileName, left);
                tree.nodes.put(right.fileName, right);

                //设置链接
                if (previous != null){
                    previous.setNext(left.fileName);
                    left.setPrevious(previous.fileName);
                }
                if (next != null) {
                    next.setPrevious(right.fileName);
                    right.setNext(next.fileName);
                }
                if (previous == null){
                    tree.setHead(left);
                }

                left.setNext(right.fileName);
                right.setPrevious(left.fileName);

                //previousFileName = null;
                //nextFileName = null;

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
                    int index = parent.getChildren().indexOf(this.fileName);
                    parent.getChildren().remove(this.fileName);
                    left.setParent(parent.fileName);
                    right.setParent(parent.fileName);
                    parent.getChildren().add(index,left.fileName);
                    parent.getChildren().add(index + 1, right.fileName);

                    //setEntries(null);
                    //setChildren(null);

                    //父节点插入或更新关键字
                    parent.updateInsert(tree);

                    // setParent(null);
                    //如果是根节点
                }else {
                    isRoot = false;
                    Node newParent = new Node(false, true);
                    tree.nodes.put(newParent.fileName, newParent);

                    tree.setRoot(newParent);
                    left.setParent(newParent.fileName);
                    right.setParent(newParent.fileName);

                    newParent.getChildren().add(left.fileName);
                    newParent.getChildren().add(right.fileName);

                    //setEntries(null);
                    //setChildren(null);

                    //更新根节点
                    newParent.updateInsert(tree);
                }
                tree.nodes.remove(this.fileName);
                tree.deletedFiles.add(this.fileName);
            }
        }
        //如果不是叶子节点
        else {
            //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                getNodeByFileName(childrenFileNames.get(0),tree).insertOrUpdate(key, obj, tree);
                //如果key大于节点最右边的key，沿最后一个子节点继续搜索
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                getNodeByFileName(childrenFileNames.get(childrenFileNames.size()-1),tree).insertOrUpdate(key, obj, tree);
                //否则沿比key大的前一个子节点继续搜索
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        getNodeByFileName(childrenFileNames.get(i),tree).insertOrUpdate(key, obj, tree);
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
        if (childrenFileNames.size() > tree.getOrder()) {
            //分裂成左右两个节点
            Node left = new Node(false);
            Node right = new Node(false);

            tree.nodes.put(left.fileName, left);
            tree.nodes.put(right.fileName, right);

            //左右两个节点关键字长度
            int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
            int rightSize = (tree.getOrder() + 1) / 2;
            //复制子节点到分裂出来的新节点，并更新关键字
            for (int i = 0; i < leftSize; i++){
                left.getChildren().add(childrenFileNames.get(i));
                left.getEntries().add(new SimpleEntry(getNodeByFileName(childrenFileNames.get(i),tree).getEntries().get(0).getKey(), null));
                getNodeByFileName(childrenFileNames.get(i),tree).setParent(left.fileName);
            }
            for (int i = 0; i < rightSize; i++){
                right.getChildren().add(childrenFileNames.get(leftSize + i));
                right.getEntries().add(new SimpleEntry(getNodeByFileName(childrenFileNames.get(leftSize + i),tree).getEntries().get(0).getKey(), null));
                getNodeByFileName(childrenFileNames.get(leftSize + i),tree).setParent(right.fileName);
            }

            //如果不是根节点
            if (parentFileName != null) {
                Node parent = getNodeByFileName(parentFileName,tree);
                //调整父子节点关系
                int index = parent.getChildren().indexOf(this.fileName);
                parent.getChildren().remove(this.fileName);
                left.setParent(parent.fileName);
                right.setParent(parent.fileName);
                parent.getChildren().add(index,left.fileName);
                parent.getChildren().add(index + 1, right.fileName);

                //setEntries(null);
                //setChildren(null);

                //父节点更新关键字
                parent.updateInsert(tree);

                setParent(null);
                //如果是根节点
            }else {
                isRoot = false;
                Node parent = new Node(false, true);
                tree.nodes.put(parent.fileName, parent);

                tree.setRoot(parent);
                left.setParent(parent.fileName);
                right.setParent(parent.fileName);
                parent.getChildren().add(left.fileName);
                parent.getChildren().add(right.fileName);

                //setEntries(null);
                //setChildren(null);

                //更新根节点
                parent.updateInsert(tree);
            }
            tree.nodes.remove(this.fileName);
            tree.deletedFiles.add(this.fileName);
        }

    }

    /** 调整节点关键字*/
    protected void validate(Node node, BplusTree tree) {

        // 如果关键字个数与子节点个数相同
        if (node.getEntries().size() == node.getChildren().size()) {
            for (int i = 0; i < node.getEntries().size(); i++) {
                Comparable key = getNodeByFileName(node.getChildren().get(i),tree).getEntries().get(0).getKey();
                if (node.getEntries().get(i).getKey().compareTo(key) != 0) {
                    node.getEntries().remove(i);
                    node.getEntries().add(i, new SimpleEntry(key, null));
                    if(!node.isRoot()){
                        validate(getNodeByFileName(node.getParentFileName(),tree), tree);
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
                Comparable key = getNodeByFileName(node.getChildren().get(i),tree).getEntries().get(0).getKey();
                node.getEntries().add(new SimpleEntry(key, null));
            }
            if (!node.isRoot()) {
                validate(getNodeByFileName(node.getParentFileName(),tree), tree);
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
    public void setPrevious(String previousFileName) {
        this.previousFileName = previousFileName;
    }

    public Node getNext(BplusTree tree) {
        return getNodeByFileName(nextFileName,tree);
    }

    public void setNext(String nextFileName) {
        this.nextFileName = nextFileName;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    public String getParentFileName() {
        return parentFileName;
    }

    public void setParent(String parentFileName) {
        this.parentFileName = parentFileName;
    }

    public List<Entry<Comparable, Object>> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry<Comparable, Object>> entries) {
        this.entries = entries;
    }

    public List<String> getChildren() {
        return childrenFileNames;
    }

    public void setChildren(List<String> children) {
        this.childrenFileNames = children;
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

    public Node getNodeByFileName(String fileName,BplusTree tree){
        Node node = null;
        node = tree.nodes.get(fileName);
        if (node == null && fileName != null){
            node = tree.readNodeFromFile(fileName,"index" + File.separator);
            tree.nodes.put(fileName, node);
        }

        return node;
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
