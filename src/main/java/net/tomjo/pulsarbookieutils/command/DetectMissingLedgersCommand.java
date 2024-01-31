package net.tomjo.pulsarbookieutils.command;

import io.quarkus.logging.Log;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.service.LedgerMetadataService;
import net.tomjo.pulsarbookieutils.service.PulsarResourcesService;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.*;

import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createPulsarResourcesService;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createZookeeperLedgerMetadataService;

@Command(name = "detect-missing-ledgers", description = "Detects missing ledgers associated with topic/namespace/tenant.")
public class DetectMissingLedgersCommand implements Runnable {
    @Option(
            names = {"-p", "--pulsar-admin"},
            description = "Pulsar admin endpoint",
            required = true
    )
    String pulsarAdminHost;
    @Option(
            names = {"--auth-plugin"},
            description = "Pulsar auth plugin"
    )
    String authPlugin;

    @Option(
            names = {"--auth-params"},
            description = "Pulsar auth params"
    )
    String authParams;

    @Option(
            names = {"--tls-trust-certs-file-path"},
            description = "Path to certificate to be trusted for TLS connection"
    )
    String tlsTrustCertsFilePath;
    @Option(
            names = {"-z", "--zookeeper"},
            description = "Zookeeper host",
            required = true
    )
    String zookeeperHost;

    @Option(
            names = {"-zt", "--zookeeper-timeout"},
            description = "Zookeeper session timeout in milliseconds"
    )
    int zookeeperTimeout = 30000;


    @Parameters(index = "0", description = "The resource to detect missing ledgers for.")
    String resource;

    private PulsarResourcesService pulsarResourcesService;

    private LedgerMetadataService ledgerMetadataService;

    public DetectMissingLedgersCommand() {
    }

    @Override
    public void run() {
        Map<String, Object> pulsarConfig = new HashMap<>();
        if (tlsTrustCertsFilePath != null) {
            pulsarConfig.put("tlsTrustCertsFilePath", tlsTrustCertsFilePath);
        }
        try {
            this.pulsarResourcesService = createPulsarResourcesService(pulsarAdminHost, authPlugin, authParams, pulsarConfig).get();
            this.ledgerMetadataService = createZookeeperLedgerMetadataService(zookeeperHost, zookeeperTimeout).get();
            detectMissingLedgers();
        } finally {
            Try.run(this.pulsarResourcesService::close);
            Try.run(this.ledgerMetadataService::close);
        }
    }

    private void detectMissingLedgers() {
        List<Long> topicLedgers = new LinkedList<>();
        List<String> topics = pulsarResourcesService.listTopics(resource);
        topics.stream()
                .map(pulsarResourcesService::getLedgersUsedByTopic)
                .map(f -> f.andThen(ledgers -> topicLedgers.addAll(ledgers.get())))
                .forEach(Future::await);
        topicLedgers.removeAll(ledgerMetadataService.listLedgers().get().get());
        Log.info("Detected missing ledger in the topics " + Arrays.toString(topics.toArray()) + ": " + Arrays.toString(topicLedgers.toArray()));
    }
}
