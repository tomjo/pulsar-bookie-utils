package net.tomjo.pulsarbookieutils.command;

import io.quarkus.logging.Log;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.Ledger;
import net.tomjo.pulsarbookieutils.Util;
import net.tomjo.pulsarbookieutils.service.LedgerMetadataService;
import net.tomjo.pulsarbookieutils.service.PulsarResourcesService;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static net.tomjo.pulsarbookieutils.service.LedgerMetadataService.*;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createPulsarResourcesService;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createZookeeperLedgerMetadataService;

@Command(name = "deep-clean", description = "Deletes all data associated with topic (topic, ledgers, cursors, ...) from BookKeeper and ZooKeeper.")
public class DeepCleanCommand implements Runnable {

    public static final List<String> METADATA_SCAN_PATHS = List.of(SCHEMAS, NAMESPACES, MANAGED_LEDGERS, BUNDLE_DATA);
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
            names = {"-d", "--dry-run"},
            description = {"Only log the resources to be cleaned"}
    )
    boolean dryRun = false;

    @Option(
            names = {"-f", "--force"},
            description = {"Force clean all resources detected"}
    )
    boolean force = false;

    @CommandLine.Parameters(index = "0", description = "The resource to deep clean.")
    String resource;


    private PulsarResourcesService pulsarResourcesService;

    private LedgerMetadataService ledgerMetadataService;

    public DeepCleanCommand() {
    }


    @Override
    public void run() {
        if (Util.isTopic(resource)) {
            Log.error("Resource should be a tenant or a namespace");
            System.exit(1);
        }
        resource = this.resource.replace(TopicDomain.persistent.name() + "://", "");

        Map<String, Object> pulsarConfig = new HashMap<>();
        if (tlsTrustCertsFilePath != null) {
            pulsarConfig.put("tlsTrustCertsFilePath", tlsTrustCertsFilePath);
        }
        try {
            this.pulsarResourcesService = createPulsarResourcesService(pulsarAdminHost, authPlugin, authParams, pulsarConfig).get();
            this.ledgerMetadataService = createZookeeperLedgerMetadataService(zookeeperHost, zookeeperTimeout).get();
            cleanLedgers();
            cleanMetadata();
        } finally {
            Try.run(this.pulsarResourcesService::close);
            Try.run(this.ledgerMetadataService::close);
        }
    }

    private void cleanLedgers() {
        CopyOnWriteArrayList<Long> ledgersToDelete = new CopyOnWriteArrayList<>();
        Map<TopicName, List<Ledger>> ledgerTopicMapping = ledgerMetadataService.listLedgerMetadata()
                .map(Future::get)
                .map(ledgerMetadata -> ledgerMetadata.values()
                        .stream()
                        .map(Ledger::new)
                        .filter(Ledger::isPulsarLedger)
                        .collect(HashMap::new,
                                (Map<TopicName, List<Ledger>> m, Ledger v) -> m.merge(v.getLedgerTopic().orElse(null), List.of(v), (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).toList()),
                                Map::putAll))
                .get();

        List<String> topics = pulsarResourcesService.listTopics(resource);
        topics.forEach(topic -> {
            ledgersToDelete.addAll(pulsarResourcesService.getLedgersUsedByTopic(topic).get());
            ledgersToDelete.addAll(getLedgersAssociatedWithTopic(ledgerTopicMapping, topic));
        });
        if (ledgersToDelete.isEmpty()) {
            Log.info("No ledgers found for " + resource);
        } else {
            Log.info("Detected ledgers: " + ledgersToDelete);
        }

        if (!dryRun) {
            List<Long> failedToDeleteLedgers = deleteLedgersReturningFailedToDeleteLedgers(ledgersToDelete);
            Log.info("Deleted " + (ledgersToDelete.size() - failedToDeleteLedgers.size()) + " ledgers");
            if (!failedToDeleteLedgers.isEmpty()) {
                Log.error("Failed to delete ledgers: " + failedToDeleteLedgers);
                if (!force) {
                    System.exit(1);
                }
            }
        }
    }

    private List<Long> deleteLedgersReturningFailedToDeleteLedgers(CopyOnWriteArrayList<Long> ledgersToDelete) {
        List<Long> failedToDeleteLedgers = new ArrayList<>();
        ledgersToDelete.stream()
                .map(ledger -> ledgerMetadataService.deleteLedger(ledger)
                        .onFailure(e -> {
                            Log.error("Could not delete ledger or ledger metadata", e);
                            failedToDeleteLedgers.add(ledger);
                        }))
                .forEach(Future::get);
        return failedToDeleteLedgers;
    }

    private void cleanMetadata() {
        List<String> metadataPathsToDelete = scanMetadata();
        if (metadataPathsToDelete.isEmpty()) {
            Log.info("No metadata paths found for " + resource);
        } else {
            Log.info("Detected metadata paths: " + metadataPathsToDelete);
        }
        if (!dryRun) {
            List<String> failedToDeleteMetadataPaths = deleteMetadataPathsRecursiveReturningFailedToDeleteMetadataPaths(metadataPathsToDelete);
            Log.info("Deleted metadata paths: " + getDeletedMetadataPaths(metadataPathsToDelete, failedToDeleteMetadataPaths));
            if (!failedToDeleteMetadataPaths.isEmpty()) {
                Log.warn("Failed to delete metadata paths: " + failedToDeleteMetadataPaths);
            }
        }
    }

    private List<String> deleteMetadataPathsRecursiveReturningFailedToDeleteMetadataPaths(List<String> metadataPathsToDelete) {
        List<String> failedToDeleteMetadataPaths = new ArrayList<>();
        metadataPathsToDelete.stream()
                .map(path -> ledgerMetadataService.deletePathRecursive(path)
                        .onFailure(e -> {
                            Log.error("Could not delete metadata path" + path, e);
                            failedToDeleteMetadataPaths.add(path);
                        }))
                .forEach(Future::get);
        return failedToDeleteMetadataPaths;
    }

    private static List<String> getDeletedMetadataPaths(List<String> metadataPathsToDelete, List<String> failedToDeleteMetadataPaths) {
        List<String> deletedMetadataPaths = new ArrayList<>(metadataPathsToDelete);
        deletedMetadataPaths.removeAll(failedToDeleteMetadataPaths);
        return deletedMetadataPaths;
    }

    private List<String> scanMetadata() {
        List<Future<Optional<String>>> metadataPathsToDelete = new ArrayList<>();
        METADATA_SCAN_PATHS.forEach(path -> metadataPathsToDelete.add(scanPath(path + resource)));
        return awaitFutures(metadataPathsToDelete).stream()
                .flatMap(Optional::stream)
                .toList();
    }

    private Future<Optional<String>> scanPath(String path) {
        return ledgerMetadataService.existsPath(path)
                .map(exists -> exists ? Optional.of(path) : Optional.empty());
    }

    private static <T> List<T> awaitFutures(List<Future<T>> futures) {
        return futures.stream()
                .map(Future::get)
                .toList();
    }

    private List<Long> getLedgersAssociatedWithTopic(Map<TopicName, List<Ledger>> ledgerTopicMapping, String topic) {
        return ledgerTopicMapping.get(TopicName.get(topic))
                .stream()
                .map(Ledger::getLedgerId)
                .toList();
    }

}
