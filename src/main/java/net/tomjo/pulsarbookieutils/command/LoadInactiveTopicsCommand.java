package net.tomjo.pulsarbookieutils.command;

import io.quarkus.logging.Log;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.service.LedgerMetadataService;
import net.tomjo.pulsarbookieutils.service.PulsarResourcesService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.tomjo.pulsarbookieutils.service.LedgerMetadataService.MANAGED_LEDGERS;
import static net.tomjo.pulsarbookieutils.service.LedgerMetadataService.METADATASTORE_TIMEOUT_MS;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createPulsarResourcesService;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createZookeeperLedgerMetadataService;

@Command(name = "load-inactive-topics", description = "Load inactive topics older than threshold for namespace - this can be used to trigger their retention policy, triggering cleanup")
public class LoadInactiveTopicsCommand implements Runnable {
    private static final long DAY_IN_MILLIS = 86400000L;
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

    @Option(
            names = {"--inactive-days"},
            description = {"Minimum days inactive. Default 10 days"}
    )
    int inactiveDaysThreshold = 10;

    @CommandLine.Parameters(index = "0", description = "The namespace whose inactive topics to load.")
    String namespace;

    @Option(
            names = {"-d", "--dry-run"},
            description = {"Only log the eligible inactive topics"}
    )
    boolean dryRun = false;

    private final Clock clock;
    private PulsarResourcesService pulsarResourcesService;

    private LedgerMetadataService ledgerMetadataService;

    public LoadInactiveTopicsCommand(Clock clock) {
        this.clock = clock;
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
            loadInactiveTopics();
        } finally {
            Try.run(this.pulsarResourcesService::close);
            Try.run(this.ledgerMetadataService::close);
        }
    }

    private void loadInactiveTopics() {
        Try.of(() -> pulsarResourcesService.namespaces().getTopics(namespace))
                .get().stream()
                .filter(topic -> Try.of(() -> isTopicInactive(topic, inactiveDaysThreshold * DAY_IN_MILLIS)).get())
                .peek(topic -> Log.info("Loading inactive topic: " + topic))
                .filter(topic -> !dryRun)
                .forEach(topic -> Try.run(() -> loadTopic(topic)).get());
    }

    private boolean isTopicInactive(String topic, long inactiveMillisThreshold) {
        TopicName topicName = TopicName.get(topic);
        if (topicName.isPersistent()) {
            String metadataPath = MANAGED_LEDGERS + topicName.getPersistenceNamingEncoding();
            return ledgerMetadataService.getLedgerStats(metadataPath)
                    .await(METADATASTORE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .getValue()
                    .get()
                    .map(stat -> stat.isPresent() && clock.millis() - stat.get().getModificationTimestamp() > inactiveMillisThreshold)
                    .get();
        }
        return false;
    }

    private void loadTopic(String topic) throws PulsarAdminException {
        pulsarResourcesService.topics().getLastMessageId(topic);
    }
}
