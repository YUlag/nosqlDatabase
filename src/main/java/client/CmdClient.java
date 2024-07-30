package client;

import org.apache.commons.cli.*;
import service.NormalStore;

import java.io.IOException;

public class CmdClient {
    private NormalStore store;

    public CmdClient() {
        try {
            this.store = new NormalStore();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        CmdClient cmdClient = new CmdClient();
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        options.addOption("c", "command", true, "Command (set, get, rm, close)")
                .addOption("k", "key", true, "Key")
                .addOption("v", "value", true, "Value");

        try {
            CommandLine cmd = parser.parse(options, args);
            String command = cmd.getOptionValue("c");

            if (command == null) {
                printUsage(options);
                return;
            }

            switch (command) {
                case "set":
                    if (!cmd.hasOption("k") || !cmd.hasOption("v")) {
                        printUsage(options);
                        return;
                    }
                    cmdClient.store.set(cmd.getOptionValue("k"), cmd.getOptionValue("v"));
                    break;

                case "get":
                    if (!cmd.hasOption("k")) {
                        printUsage(options);
                        return;
                    }
                    String value = cmdClient.store.get(cmd.getOptionValue("k"));
                    System.out.println("Value: " + value);
                    break;

                case "rm":
                    if (!cmd.hasOption("k")) {
                        printUsage(options);
                        return;
                    }
                    cmdClient.store.rm(cmd.getOptionValue("k"));
                    break;

                case "close":
                    cmdClient.store.close();
                    break;

                case "help":
                default:
                    printUsage(options);
                    break;
            }
        } catch (ParseException e) {
            System.err.println("Failed to parse command line arguments");
            printUsage(options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CmdClient", options);
    }
}
