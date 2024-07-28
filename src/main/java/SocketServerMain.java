/*
 *@Type SocketServerUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:08
 * @version
 */


import controller.SocketServerController;
import service.NormalStore;
import service.Store;

import java.io.File;
import java.io.IOException;

public class SocketServerMain { //通过Socket操作Store C/S
    public static final int storeThreshold = 1000;
    public static void main(String[] args) throws ClassNotFoundException {
        String host = "localhost";
        int port = 12345;
        String dataDir = "data"+ File.separator;
        SocketServerController controller;
        try (Store store = new NormalStore(dataDir, storeThreshold)) {
            controller = new SocketServerController(host, port, store);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        controller.startServer();
    }
}
