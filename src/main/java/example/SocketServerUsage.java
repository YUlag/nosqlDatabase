/*
 *@Type SocketServerUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:08
 * @version
 */
package example;

import controller.SocketServerController;
import service.NormalStore;
import service.Store;

import java.io.File;

public class SocketServerUsage { //通过Socket操作Store C/S
    public static final int storeThreshold = 1000;
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        String dataDir = "data"+ File.separator;
        Store store = new NormalStore(dataDir,storeThreshold);
        SocketServerController controller = new SocketServerController(host, port, store);
        controller.startServer();
    }
}
