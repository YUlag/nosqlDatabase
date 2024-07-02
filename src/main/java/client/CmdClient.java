package client;
/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */

public class CmdClient {
    public static void main(String[] args) {
        if (args.length < 3) {
            printUsage();
            return;
        }

        String host = args[0];
        int port;
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Port must be a number.");
            printUsage();
            return;
        }

        Client client = new SocketClient(host, port);

        String command = args[2];
        switch (command) {
            case "set":
                if (args.length != 5) {
                    printUsage();
                    return;
                }
                client.set(args[3], args[4]);
                break;

            case "get":
                if (args.length != 4) {
                    printUsage();
                    return;
                }
                client.get(args[3]);
                break;

            case "rm":
                if (args.length != 4) {
                    printUsage();
                    return;
                }
                client.rm(args[3]);
                break;

            case "close":
                if (args.length != 3) {
                    printUsage();
                    return;
                }
                client.close();
                break;

            case "help":
            case "-h":
            default:
                printUsage();
                break;
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("java SocketClientMain <host> <port> set <key> <value>");
        System.out.println("java SocketClientMain <host> <port> get <key>");
        System.out.println("java SocketClientMain <host> <port> rm <key>");
        System.out.println("java SocketClientMain <host> <port> close");
    }
}
