package net.tomjo.pulsarbookieutils.service;

import io.quarkus.logging.Log;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

import java.io.Closeable;
import java.util.*;

import static net.tomjo.pulsarbookieutils.Util.isNamespace;
import static net.tomjo.pulsarbookieutils.Util.isTopic;

public class PulsarResourcesService implements Closeable {

    private final PulsarAdmin pulsarAdmin;

    public PulsarResourcesService(PulsarAdmin pulsarAdmin) {
        this.pulsarAdmin = pulsarAdmin;
    }

    public Topics topics() {
        return pulsarAdmin.topics();
    }

    public TopicPolicies topicPolicies() {
        return pulsarAdmin.topicPolicies();
    }

    public Namespaces namespaces() {
        return pulsarAdmin.namespaces();
    }

    public List<String> listTopics(String pulsarResourceIdentifier) {
        List<String> topics = new LinkedList<>();
        if (isTopic(pulsarResourceIdentifier)) {
            topics.add(pulsarResourceIdentifier);
        } else if (isNamespace(pulsarResourceIdentifier)) {
            Try.of(() -> pulsarAdmin.namespaces().getTopics(pulsarResourceIdentifier))
                    .recover(e -> {
                        Log.error("Could not derive topics from namespace " + pulsarResourceIdentifier + ": " + e.getMessage(), e);
                        return List.of();
                    })
                    .andThen(topics::addAll);
        } else {
            Try.of(() -> pulsarAdmin.namespaces().getNamespaces(pulsarResourceIdentifier))
                    .recover(e -> {
                        Log.error("Could not derive topics from tenant " + pulsarResourceIdentifier + ": " + e.getMessage(), e);
                        return List.of();
                    })
                    .forEach(namespaces -> namespaces.stream()
                            .map(namespace -> Try.of(() -> pulsarAdmin.namespaces().getTopics(namespace))
                                    .recover(e -> {
                                        Log.error("Could not derive topics from namespace " + namespace + ": " + e.getMessage(), e);
                                        return List.of();
                                    })
                                    .get())
                            .forEach(topics::addAll));
        }
        return topics;
    }

    public Future<Set<Long>> getLedgersUsedByTopic(String topic) {
        return Future.fromCompletableFuture(topics()
                .getInternalStatsAsync(topic)
                .thenApply(stats -> {
                    Set<Long> ledgers = new TreeSet<>(getLedgerIds(stats.ledgers));
                    ledgers.addAll(getLedgerIds(stats.schemaLedgers));
                    ledgers.addAll(getLedgerIds(List.of(stats.compactedLedger)));
                    ledgers.addAll(getCursorLedgerIds(stats.cursors.values()));
                    return ledgers;
                }));
    }

    private List<Long> getCursorLedgerIds(Collection<ManagedLedgerInternalStats.CursorStats> cursors) {
        return cursors.stream()
                .map(cursor -> cursor.cursorLedger)
                .filter(ledgerId -> ledgerId > -1)
                .toList();
    }

    private List<Long> getLedgerIds(Collection<ManagedLedgerInternalStats.LedgerInfo> ledgers) {
        return ledgers.stream()
                .map(ledger -> ledger.ledgerId)
                .filter(ledgerId -> ledgerId > -1)
                .toList();
    }

    @Override
    public void close() {
        Try.run(this.pulsarAdmin::close);
    }
}
