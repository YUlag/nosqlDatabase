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
////
//            for (int i = 0; i < 100; i++){ // 阈值959
//                store.rm("fzx" + i);
//                sleep(100);
//                System.out.println("rm " + i);
//            }
//            store.set("zsy1", "1");
//            store.set("zsy2", "2");
//            store.set("zsy3", "3");
//            store.set("zsy1", "你好");
//            store.set("fzx", "wlj");
//            store.set("zsy1", "hello");
//            store.rm("zsy1");
//            store.set("zsy4", "4");
//            store.rm("zsy4");
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
