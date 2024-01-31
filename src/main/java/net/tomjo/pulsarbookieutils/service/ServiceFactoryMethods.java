package net.tomjo.pulsarbookieutils.service;

import io.quarkus.logging.Log;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.Ledger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ServiceFactoryMethods {
    public static Try<PulsarResourcesService> createPulsarResourcesService(String pulsarAdminHost, String authPlugin, String authParams, Map<String, Object> pulsarConfig) {
        return Try.of(() -> PulsarAdmin.builder()
                        .loadConf(pulsarConfig)
                        .serviceHttpUrl(pulsarAdminHost)
                        .authentication(authPlugin, authParams)
                        .build())
                .map(PulsarResourcesService::new);
    }

    public static Try<LedgerMetadataService> createZookeeperLedgerMetadataService(String zookeeperHost, int sessionTimeoutMs) {
        Try<LedgerManagerFactory> ledgerManagerFactory = createZookeeperLedgerManagerFactory(zookeeperHost, sessionTimeoutMs);
        Try<MetadataStore> zookeeperMetadataStore = createZookeeperMetadataStore(zookeeperHost, sessionTimeoutMs);
        return ledgerManagerFactory
                .flatMap(lmf -> zookeeperMetadataStore
                        .map(ms -> new LedgerMetadataService(lmf, ms)));
    }

    private static Try<LedgerManagerFactory> createZookeeperLedgerManagerFactory(String zookeeperHost, int sessionTimeoutMs) {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setMetadataServiceUri("zk+null://" + zookeeperHost + Ledger.LEDGER_ROOT);
        serverConfiguration.setZkTimeout(sessionTimeoutMs);
        return Try.of(serverConfiguration::getMetadataServiceUri)
                .map(URI::create)
                .map(MetadataDrivers::getBookieDriver)
                .andThenTry(metadataDriver -> metadataDriver.initialize(serverConfiguration, NullStatsLogger.INSTANCE))
                .mapTry(MetadataBookieDriver::getLedgerManagerFactory);
    }

    private static Try<MetadataStore> createZookeeperMetadataStore(String zookeeperHost, int sessionTimeoutMs) {
        CompletableFuture<Void> zookeeperFuture = new CompletableFuture<>();
        return Try.of(() -> new ZooKeeper(zookeeperHost, sessionTimeoutMs, (watchedEvent1) -> {
                    if (watchedEvent1.getState() == Watcher.Event.KeeperState.SyncConnected) {
                        zookeeperFuture.complete(null);
                    }
                }))
                .onFailure(zookeeperFuture::completeExceptionally)
                .flatMap(z -> Future.fromCompletableFuture(zookeeperFuture)
                        .await(sessionTimeoutMs, TimeUnit.MILLISECONDS)
                        .toTry()
                        .map(v -> (MetadataStore) new ZKMetadataStore(z))
                )
                .onFailure(ex -> Log.error("Could not connect to zookeeper " + zookeeperHost + ": " + ex.getMessage(), ex));
    }
}
