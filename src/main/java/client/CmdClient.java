package client;

import org.apache.commons.cli.*;
import service.NormalStore;

import java.io.IOException;

import static java.lang.System.exit;

public class CmdClient {
    public static void main(String[] args) {
        try (NormalStore store = new NormalStore()) {
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
                        store.set(cmd.getOptionValue("k"), cmd.getOptionValue("v"));
                        System.out.println("Key set");
                        break;

                    case "get":
                        if (!cmd.hasOption("k")) {
                            printUsage(options);
                            return;
                        }
                        String value = store.get(cmd.getOptionValue("k"));
                        System.out.println("Value: " + value);
                        break;

                    case "rm":
                        if (!cmd.hasOption("k")) {
                            printUsage(options);
                            return;
                        }
                        store.rm(cmd.getOptionValue("k"));
                        System.out.println("Key removed");
                        break;

                    case "close":
                        store.close();
                        break;

                    case "help":
                    default:
                        printUsage(options);
                        break;
                }
                exit(0);
            } catch (ParseException e) {
                System.err.println("Failed to parse command line arguments");
                printUsage(options);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }finally {
                store.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CmdClient", options);
    }
}
