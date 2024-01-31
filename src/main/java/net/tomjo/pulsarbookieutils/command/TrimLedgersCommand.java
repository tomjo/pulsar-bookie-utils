package net.tomjo.pulsarbookieutils.command;

import io.quarkus.logging.Log;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.Ledger;
import net.tomjo.pulsarbookieutils.service.LedgerMetadataService;
import net.tomjo.pulsarbookieutils.service.PulsarResourcesService;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Comparator.comparingLong;
import static net.tomjo.pulsarbookieutils.service.LedgerMetadataService.METADATASTORE_TIMEOUT_MS;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createPulsarResourcesService;
import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createZookeeperLedgerMetadataService;

@Command(name = "trim-ledgers", description = "Trim the oldest existing ledgers to free up space, either by amount of ledgers or by date. By default only considers expired ledgers.")
public class TrimLedgersCommand implements Runnable {

    private static final long SECONDS_IN_MINUTE = 60L;
    private static final long MS_IN_SECOND = 1000L;

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

    @CommandLine.ArgGroup(
            multiplicity = "1"
    )
    LedgerSelectionOptions ledgerSelectionOptions;

    @Option(
            names = {"-d", "--dry-run"},
            description = {"Only log the ledgers eligible for trimming, don't actually delete them"}
    )
    boolean dryRun = false;

    @Option(
            names = {"-f", "--force"},
            description = {"Force trim ledgers even though not expired according to retention policies"}
    )
    boolean force = false;

    private final Clock clock;

    private LedgerMetadataService ledgerMetadataService;
    private PulsarResourcesService pulsarResourcesService;


    public TrimLedgersCommand(Clock clock) {
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
            trimLedgersChronologically();
        } finally {
            Try.run(this.pulsarResourcesService::close);
            Try.run(this.ledgerMetadataService::close);
        }
    }

    public void trimLedgersChronologically() {
        List<Ledger> ledgersToTrim = getLedgersInTrimRange();
        ledgersToTrim.stream()
                .filter(ledger -> isLedgerExpired(ledger).map(b -> b || force).get())
                .peek(this::logLedgerToTrim)
                .filter(ledger -> !dryRun)
                .forEach(ledger -> ledgerMetadataService.deleteLedger(ledger.getLedgerId())
                        .onSuccess(v -> Log.info("Deleted ledger " + ledger.getLedgerId()))
                        .onFailure(ex -> Log.error("Could not delete ledger " + ledger.getLedgerId() + ": " + ex.getMessage(), ex)));
    }

    private Try<Boolean> isLedgerExpired(Ledger ledger) {
        return Try.of(() -> ledger.getLedgerTopic().orElseThrow(() -> new RuntimeException("No topic can be inferred from ledger " + ledger.getLedgerId())))
                .flatMap(topicName -> isLedgerExpiredByTopicPolicy(topicName, ledger.getCtime())
                        .flatMap(b -> b
                                ? Try.success(true)
                                : isLedgerExpiredByNamespacePolicy(topicName, ledger.getCtime())));
    }

    private Try<Boolean> isLedgerExpiredByNamespacePolicy(TopicName ledgerTopic, long ledgerCreationTime) {
        return Try.of(() -> isLedgerExpiredAccordingToRetentionTime(pulsarResourcesService.namespaces().getRetention(ledgerTopic.getNamespace()).getRetentionTimeInMinutes(), ledgerCreationTime));
    }

    private Try<Boolean> isLedgerExpiredByTopicPolicy(TopicName ledgerTopic, long ledgerCreationTime) {
        return Try.of(() -> isLedgerExpiredAccordingToRetentionTime(pulsarResourcesService.topicPolicies().getRetention(ledgerTopic.toString()).getRetentionTimeInMinutes(), ledgerCreationTime));
    }

    private boolean isLedgerExpiredAccordingToRetentionTime(long retentionTimeInMinutes, long ledgerCreationTime) {
        return retentionTimeInMinutes != -1 && (retentionTimeInMinutes == 0 || clock.millis() - ledgerCreationTime > (retentionTimeInMinutes * SECONDS_IN_MINUTE * MS_IN_SECOND));
    }

    private void logLedgerToTrim(Ledger ledger) {
        boolean expired = isLedgerExpired(ledger).get();
        boolean inMetaStore = ledgerMetadataService.existsInMetaStore(ledger).await(METADATASTORE_TIMEOUT_MS, TimeUnit.MILLISECONDS).getValue().get().get();
        Log.info("Eligible ledger for trimming: " + ledger + " |  expired: " + expired + " inMetaStore: " + inMetaStore);
    }

    private List<Ledger> getLedgersInTrimRange() {
        Map<Long, LedgerMetadata> ledgerMetadata = ledgerMetadataService.listLedgerMetadata()
                .map(Future::get)
                .get();
        List<Long> sortedLedgerIds = ledgerMetadata.entrySet().stream()
                .sorted(comparingLong(e -> e.getValue().getCtime()))
                .map(Map.Entry::getKey)
                .toList();
        if (ledgerSelectionOptions.trimBeforeDate == null) {
            return sortedLedgerIds.subList(0, ledgerSelectionOptions.amount).stream()
                    .map(ledgerMetadata::get)
                    .map(Ledger::new)
                    .filter(Ledger::isPulsarLedger)
                    .toList();
        } else {
            return sortedLedgerIds.stream()
                    .map(ledgerMetadata::get)
                    .filter(lm -> LocalDateTime.ofInstant(Instant.ofEpochMilli(lm.getCtime()), clock.getZone()).isBefore(ledgerSelectionOptions.trimBeforeDate))
                    .map(Ledger::new)
                    .filter(Ledger::isPulsarLedger)
                    .toList();
        }
    }

    private static class LedgerSelectionOptions {
        @Option(
                names = {"-n, --amount"},
                description = {"Amount of ledgers to trim. Default 10"}
        )
        int amount = 10;

        @Option(
                names = {"-b, --before-date"},
                description = {"Amount of ledgers to trim. Default 10"}
        )
        LocalDateTime trimBeforeDate;

        LedgerSelectionOptions() {
        }

    }
}
