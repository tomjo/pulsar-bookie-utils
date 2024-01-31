package net.tomjo.pulsarbookieutils.command;

import io.quarkus.logging.Log;
import io.vavr.collection.Stream;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.Ledger;
import net.tomjo.pulsarbookieutils.service.LedgerMetadataService;
import net.tomjo.pulsarbookieutils.service.PulsarResourcesService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createPulsarResourcesService;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createZookeeperLedgerMetadataService;

@Command(name = "clean-orphan-ledgers", description = "Cleans up 'orphan' ledgers (ledgers in BookKeeper but not in ZooKeeper). Minimal age to be considered orphaned is configurable.")
public class CleanOrphanLedgersCommand implements Runnable {

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
            names = {"--min-orphan-age"},
            description = {"Minimum orphan ledger age in days. Default 10 days"}
    )
    long minimumOrphanAge = 10;

    @Option(
            names = {"-d", "--dry-run"},
            description = {"Only log the ledgers eligible for cleanup, don't actually delete them"}
    )
    boolean dryRun = false;

    private final Clock clock;
    private PulsarResourcesService pulsarResourcesService;

    private LedgerMetadataService ledgerMetadataService;

    public CleanOrphanLedgersCommand(Clock clock) {
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
            cleanOrphanedLedgers();
        } finally {
            Try.run(this.pulsarResourcesService::close);
            Try.run(this.ledgerMetadataService::close);
        }
    }

    private void cleanOrphanedLedgers() {
        Map<Long, Ledger> pulsarLedgers = findPulsarLedgers();
        Map<Long, Set<Long>> pulsarLedgerAssociatedLedgersMapping = findUsedLedgersAccordingToInternalTopicStats(pulsarLedgers);
        Set<Long> nonOrphanedLedgers = getNonOrphanedLedgers(pulsarLedgerAssociatedLedgersMapping);

        List<Ledger> orphanedLedgersDueToMissingTopic = findOrphanedLedgersDueToMissingTopic(pulsarLedgerAssociatedLedgersMapping, pulsarLedgers);
        List<Ledger> orphanedLedgersNotLinkedToATopic = findOrphanedLedgersNotLinkedToATopic(pulsarLedgers);
        List<Ledger> orphanedLedgersLinkedToATopic = findOrphanedLedgersLinkedToATopic(pulsarLedgerAssociatedLedgersMapping, pulsarLedgers);
        List<Ledger> orphanedLedgersDueToMissingInMetadataStore = findOrphanedLedgersDueToMissingInMetadataStore(pulsarLedgers);

        Stream.concat(orphanedLedgersDueToMissingTopic,
                        orphanedLedgersNotLinkedToATopic,
                        orphanedLedgersLinkedToATopic,
                        orphanedLedgersDueToMissingInMetadataStore)
                .distinct()
                .filter(l -> !nonOrphanedLedgers.contains(l.getLedgerId()))
                .map(this::deleteOrphanedLedgerIfAgeThresholdMet)
                .forEach(Future::get);
    }

    private static List<Ledger> findOrphanedLedgersLinkedToATopic(Map<Long, Set<Long>> pulsarLedgerAssociatedLedgersMapping, Map<Long, Ledger> pulsarLedgers) {
        return pulsarLedgerAssociatedLedgersMapping.entrySet().stream()
                .filter(e -> !e.getValue().contains(e.getKey()))
                .map(Map.Entry::getKey)
                .map(pulsarLedgers::get)
                .toList();
    }

    private List<Ledger> findOrphanedLedgersDueToMissingTopic(Map<Long, Set<Long>> pulsarLedgerAssociatedLedgersMapping, Map<Long, Ledger> pulsarLedgers) {
        return pulsarLedgerAssociatedLedgersMapping.entrySet()
                .stream()
                .filter(e -> e.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .map(pulsarLedgers::get)
                .filter(this::isLedgerTopicNotFound)
                .filter(this::isOrphanedLedgerWithTopicMissing)
                .toList();
    }

    private List<Ledger> findOrphanedLedgersDueToMissingInMetadataStore(Map<Long, Ledger> pulsarLedgers) {
        return pulsarLedgers.values()
                .stream()
                .filter(ledger -> ledger.isSchemaLedger() || ledger.isManagedLedger() || ledger.isCompactedTopic())
                .filter(ledger -> !ledgerMetadataService.existsInMetaStore(ledger).get())
                .toList();
    }

    private static List<Ledger> findOrphanedLedgersNotLinkedToATopic(Map<Long, Ledger> pulsarLedgers) {
        return pulsarLedgers.values()
                .stream()
                .filter(ledger -> !(ledger.isSchemaLedger() || ledger.isManagedLedger() || ledger.isCompactedTopic()))
                .toList();
    }

    private boolean isLedgerTopicNotFound(Ledger ledger) {
        return pulsarResourcesService.getLedgersUsedByTopic(ledger.getLedgerTopic().orElseThrow().toString())
                .map(l -> false)
                .recover(e -> e.getCause() instanceof PulsarAdminException.NotFoundException)
                .getOrElse(false);
    }

    private static Set<Long> getNonOrphanedLedgers(Map<Long, Set<Long>> pulsarLedgerAssociatedLedgersMapping) {
        return pulsarLedgerAssociatedLedgersMapping.values()
                .stream()
                .flatMap(Set::stream)
                .collect(toSet());
    }

    private Map<Long, Ledger> findPulsarLedgers() {
        return ledgerMetadataService.listLedgerMetadataIncludingMissing().get().get()
                .values().stream()
                .map(Ledger::new)
                .filter(Ledger::isPulsarLedger)
                .collect(toMap(Ledger::getLedgerId, Function.identity()));
    }

    private boolean isOrphanedLedgerWithTopicMissing(Ledger ledger) {
        return !ledger.isSchemaLedger() || isOrphanedSchemaLedgerLoggingException(ledger);
    }

    private Map<Long, Set<Long>> findUsedLedgersAccordingToInternalTopicStats(Map<Long, Ledger> ledgers) {
        return ledgers.values()
                .stream()
                .filter(ledger -> ledger.isSchemaLedger() || ledger.isManagedLedger() || ledger.isCompactedTopic())
                .filter(l -> ledgerMetadataService.existsInMetaStore(l).get())
                .collect(toMap(Ledger::getLedgerId, l -> l.getLedgerTopic()
                        .map(topicName -> topicName.getPartition(0).toString())
                        .map(pulsarResourcesService::getLedgersUsedByTopic)
                        .map(f -> f.getOrElse(Set.of()))
                        .orElse(Set.of())));
    }

    private boolean isOrphanedSchemaLedgerLoggingException(Ledger ledger) {
        return isOrphanedSchemaLedger(ledger)
                .recover(ex -> {
                    Log.error("Could not read ledgers used by topic " + ledger.getSchemaLedgerTopic().orElseThrow() + " related to ledger " + ledger.getLedgerId() + ": " + ex.getMessage(), ex);
                    return false;
                })
                .get();
    }

    private Try<Boolean> isOrphanedSchemaLedger(Ledger ledger) {
        if (ledger.isSchemaLedger()) {
            TopicName schemaLedgerTopic = ledger.getSchemaLedgerTopic().orElseThrow();
            return pulsarResourcesService.getLedgersUsedByTopic(schemaLedgerTopic.getPartition(0).toString())
                    .toTry()
                    .map(ledgers -> !ledgers.contains(ledger.getLedgerId()))
                    .recoverWith(e -> {
                        if (e.getCause() instanceof PulsarAdminException.NotFoundException) {
                            return Try.of(() -> true);
                        } else {
                            return Try.failure(e);
                        }
                    });
        }
        return Try.of(() -> false);

    }

    private Future<Void> deleteOrphanedLedgerIfAgeThresholdMet(Ledger ledger) {
        if (isLedgerOldEnough(ledger)) {
            Log.info("Found orphaned ledger " + ledger.getLedgerId() + ": " + ledger);
            if (!dryRun) {
                return ledgerMetadataService.deleteLedger(ledger.getLedgerId())
                        .onSuccess(v -> Log.info("Deleted ledger " + ledger.getLedgerId()))
                        .onFailure(ex -> Log.error("Could not delete ledger " + ledger.getLedgerId() + ": " + ex.getMessage()));
            }
        }
        return Future.successful(null);
    }

    private boolean isLedgerOldEnough(Ledger ledger) {
        return clock.millis() > ledger.getCtime() + this.minimumOrphanAge * DAY_IN_MILLIS;
    }

}
