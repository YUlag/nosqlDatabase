/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */


import client.Client;
import client.SocketClient;
import utils.PropertyReaderUtil;

import java.util.Scanner;

public class SocketClientMain {
    public static void main(String[] args) {
        PropertyReaderUtil configUtil = PropertyReaderUtil.getInstance();

        String host = configUtil.getProperty("server.host");
        int port = Integer.parseInt(configUtil.getProperty("server.port"));

        Client client = new SocketClient(host, port);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(">");
            String command = scanner.nextLine();
            String[] parts = command.split(" ");
            if (parts.length == 0)
                continue;
            switch (parts[0]) {
                case "help", "-h":
                    System.out.println("Usage:\n" +
                            "set [key] [value]\n" +
                            "get [key]\n" +
                            "rm  [key]\n");
                    break;

                case "set":
                    if (parts.length != 3)
                        continue;
                    client.set(parts[1], parts[2]);
                    break;

                case "get":
                    if (parts.length != 2)
                        continue;
                    client.get(parts[1]);
                    break;

                case "rm":
                    if (parts.length != 2)
                        continue;
                    client.rm(parts[1]);
                    break;

                case "close" :
                    if (parts.length != 1)
                        continue;
                    client.close();
                    System.exit(0);
                    break;

                default:
                    ;
            }
        }
    }
}