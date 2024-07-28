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
import utils.PropertyReaderUtil;

import java.io.File;
import java.io.IOException;

public class SocketServerMain { //通过Socket操作Store C/S
    public static void main(String[] args) throws ClassNotFoundException {
        PropertyReaderUtil configUtil = PropertyReaderUtil.getInstance();

        String host = configUtil.getProperty("server.host");
        int port = Integer.parseInt(configUtil.getProperty("server.port"));

        SocketServerController controller;
        try (Store store = new NormalStore()) {
            controller = new SocketServerController(host, port, store);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        controller.startServer();
    }
}
