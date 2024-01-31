package net.tomjo.pulsarbookieutils.service;

import io.quarkus.logging.Log;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.Ledger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Stat;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Optional.ofNullable;
import static org.apache.bookkeeper.client.api.BKException.Code.NoSuchLedgerExistsException;

public class LedgerMetadataService implements Closeable {

    public static final long METADATASTORE_TIMEOUT_MS = 30000L;

    public static final String MANAGED_LEDGERS = "/managed-ledgers/";
    public static final String SCHEMAS = "/schemas/";
    public static final String NAMESPACES = "/namespace/";
    public static final String BUNDLE_DATA = "/loadbalance/bundle-data/";

    private final LedgerManagerFactory ledgerManagerFactory;

    private final MetadataStore metadataStore;

    public LedgerMetadataService(LedgerManagerFactory ledgerManagerFactory, MetadataStore metadataStore) {
        this.ledgerManagerFactory = ledgerManagerFactory;
        this.metadataStore = metadataStore;
    }

    public Future<Void> deleteLedger(long ledgerId) {
        return Try.withResources(ledgerManagerFactory::newLedgerManager)
                .of(ledgerManager -> Future.fromCompletableFuture(ledgerManager.removeLedgerMetadata(ledgerId, Version.ANY)))
                .get();
    }

    public Future<Optional<Stat>> getLedgerStats(String ledgerMetadataPath) {
        return Future.fromCompletableFuture(metadataStore.get(ledgerMetadataPath))
                .map(r -> r.map(GetResult::getStat));
    }

    public Future<Boolean> existsInMetaStore(Ledger ledger) {
        return existsAsManagedLedger(ledger)
                .zipWith(existsAsCompactedTopic(ledger), Boolean::logicalOr)
                .zipWith(existsAsSchema(ledger), Boolean::logicalOr);
    }

    public Future<Void> deletePathRecursive(String path) {
        return Future.fromCompletableFuture(metadataStore.deleteRecursive(path));
    }

    public Future<Boolean> existsPath(String path) {
        return ofNullable(path)
                .map(managedLedgerName -> metadataStore.exists(path))
                .map(Future::fromCompletableFuture)
                .orElse(Future.successful(false));
    }

    private Future<Boolean> existsAsCompactedTopic(Ledger ledger) {
        return ofNullable(ledger)
                .flatMap(info -> info.getPulsarMetadata(Ledger.COMPACTED_TOPIC))
                .map(compactedTopic -> metadataStore.exists(MANAGED_LEDGERS + TopicName.get(compactedTopic).getPersistenceNamingEncoding()))
                .map(Future::fromCompletableFuture)
                .orElse(Future.successful(false));
    }

    private Future<Boolean> existsAsSchema(Ledger ledger) {
        return ofNullable(ledger)
                .flatMap(info -> info.getPulsarMetadata(Ledger.SCHEMA_ID))
                .map(schemaId -> metadataStore.exists(SCHEMAS + schemaId))
                .map(Future::fromCompletableFuture)
                .orElse(Future.successful(false));
    }

    private Future<Boolean> existsAsManagedLedger(Ledger ledger) {
        return ofNullable(ledger)
                .flatMap(info -> info.getPulsarMetadata(Ledger.MANAGED_LEDGER))
                .map(managedLedgerName -> metadataStore.exists(MANAGED_LEDGERS + managedLedgerName))
                .map(Future::fromCompletableFuture)
                .orElse(Future.successful(false));
    }

    public Try<Future<Map<Long, LedgerMetadata>>> listLedgerMetadata() {
        return Try.withResources(ledgerManagerFactory::newLedgerManager)
                .of(this::listLedgerMetadata);
    }

    private Future<Map<Long, LedgerMetadata>> listLedgerMetadata(LedgerManager ledgerManager) {
        ConcurrentHashMap<Long, LedgerMetadata> ledgerMeta = new ConcurrentHashMap<>();
        CompletableFuture<Map<Long, LedgerMetadata>> future = new CompletableFuture<>();
        ledgerManager.asyncProcessLedgers((ledgerId, cb) -> readLedgerMetadataIgnoringExceptionsProcessor(ledgerManager, ledgerId, ledgerMeta), (rc, s, obj) -> future.complete(ledgerMeta), null, 0, -1);
        return Future.fromCompletableFuture(future);
    }

    public Try<Future<Map<Long, LedgerMetadata>>> listLedgerMetadataIncludingMissing() {
        return Try.withResources(ledgerManagerFactory::newLedgerManager)
                .of(this::listLedgerMetadataIncludingMissing);
    }

    private Future<Map<Long, LedgerMetadata>> listLedgerMetadataIncludingMissing(LedgerManager ledgerManager) {
        ConcurrentHashMap<Long, LedgerMetadata> ledgerMeta = new ConcurrentHashMap<>();
        CompletableFuture<Map<Long, LedgerMetadata>> future = new CompletableFuture<>();
        ledgerManager.asyncProcessLedgers((ledgerId, cb) -> readLedgerMetadataMappingExceptionsToNullProcessor(ledgerManager, ledgerId, ledgerMeta), (rc, s, obj) -> future.complete(ledgerMeta), null, 0, -1);
        return Future.fromCompletableFuture(future);
    }

    public Try<Future<List<Long>>> listLedgers() {
        return Try.withResources(ledgerManagerFactory::newLedgerManager)
                .of(this::listLedgers);
    }

    private Future<List<Long>> listLedgers(LedgerManager ledgerManager) {
        List<Long> ledgers = new ArrayList<>();
        CompletableFuture<List<Long>> future = new CompletableFuture<>();
        ledgerManager.asyncProcessLedgers((ledgerId, cb) -> ledgers.add(ledgerId), (rc, s, obj) -> future.complete(ledgers), null, 0, -1);
        return Future.fromCompletableFuture(future);
    }

    private void readLedgerMetadataMappingExceptionsToNullProcessor(LedgerManager ledgerManager, Long ledgerId, Map<Long, LedgerMetadata> ledgerMetadata) {
        Future.fromCompletableFuture(ledgerManager.readLedgerMetadata(ledgerId))
                .map(Versioned::getValue)
                .recover(e -> null)
                .forEach(metadata -> ledgerMetadata.put(ledgerId, metadata));
    }

    private void readLedgerMetadataIgnoringExceptionsProcessor(LedgerManager ledgerManager, Long ledgerId, Map<Long, LedgerMetadata> ledgerMetadata) {
        Future.fromCompletableFuture(ledgerManager.readLedgerMetadata(ledgerId))
                .onSuccess(versionedLegerMetadata -> ledgerMetadata.put(ledgerId, versionedLegerMetadata.getValue()))
                .onFailure(e -> {
                    if (BKException.getExceptionCode(e) == NoSuchLedgerExistsException) {
                        Log.warn(String.format("Ledger " + ledgerId + " doesn't exist."));
                    } else {
                        Log.error("Could not read ledger metadata for ledger " + ledgerId + ": " + e.getMessage(), e);
                    }
                }).get();
    }

    @Override
    public void close() {
        Try.run(this.metadataStore::close);
        Try.run(this.ledgerManagerFactory::close);
    }
}
