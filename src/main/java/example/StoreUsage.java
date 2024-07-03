/*
 *@Type Usage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 03:59
 * @version
 */
package example;

import service.NormalStore;
import utils.RandomAccessFileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static java.lang.Thread.sleep;

public class StoreUsage {
    public static void main(String[] args) {
        String dataDir = "data" + File.separator;
        try (NormalStore store = new NormalStore(dataDir, 100)) { // 自动关闭store
            for (int i = 0; i < 1000; i++){
                store.set("fzx" + i,"" + i);
                System.out.println("set " + i);
            }

            store.close();

            for (int i = 500; i < 750; i++){
                store.rm("fzx" + i);
                System.out.println("rm " + i);
            }

            store.close();

            for (int i = 750; i < 1500; i++) { //应该会在中途压缩 此时在新文件写 //950左右
                store.set("fzx" + i, "" + i);
                System.out.println("set " + i);
            }

            store.close();

            for (int i = 1500; i < 2500; i++) { //应该会在中途压缩 此时在新文件写 //950左右
                store.set("fzx" + i, "" + i);
                System.out.println("set " + i);
            }

            System.out.println(store.get("fzx" + 950));
            System.out.println(store.get("fzx" + 750));
            System.out.println(store.get("fzx" + 749));


            try {
                store.close(); // 只有关闭和超过阈值才把内存表落盘
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
