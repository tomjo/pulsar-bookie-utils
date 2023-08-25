package net.tomjo.bookieoptimize;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "bookie-optimize", mixinStandardHelpOptions = true)
public class BookieOptimizeCommand implements Runnable {

    @Option(
            names = {"-z", "--zookeeper"},
            description = "Zookeeper host",
            required = true
    )
    String zookeeperHost;

    @Option(
            names = {"-p", "--pulsar-admin"},
            description = "Pulsar admin host",
            required = true
    )
    String pulsarAdminHost;

    @Option(
            names = {"--auth-plugin"},
            description = ""
    )
    String authPlugin;

    @Option(
            names = {"--auth-plugin"},
            description = ""
    )
    String authParams;

    int age = 7;


    @Override
    public void run() {

    }
}
