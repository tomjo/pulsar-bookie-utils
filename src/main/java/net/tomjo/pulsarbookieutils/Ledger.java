package net.tomjo.pulsarbookieutils;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class Ledger {

    public static final String LEDGER_ROOT = "/ledgers";

    public static final String MANAGED_LEDGER = "pulsar/managed-ledger";
    public static final String SCHEMA_ID = "pulsar/schemaId";
    public static final String MANAGED_CURSOR = "pulsar/cursor";
    public static final String COMPACTED_TOPIC = "pulsar/compactedTopic";

    private final LedgerMetadata ledgerMetadata;
    private final Map<String, String> pulsarMetadata;

    public Ledger(LedgerMetadata ledgerMetadata) {
        this.ledgerMetadata = ledgerMetadata;
        this.pulsarMetadata = ledgerMetadata.getCustomMetadata().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, (e) -> new String(e.getValue(), StandardCharsets.UTF_8)));
    }

    public long getLedgerId() {
        return this.ledgerMetadata.getLedgerId();
    }

    public long getLength() {
        return ledgerMetadata.getLength();
    }

    public Optional<String> getPulsarMetadata(String key) {
        return Optional.ofNullable(pulsarMetadata.get(key));
    }

    public Optional<TopicName> getLedgerTopic() {
        return getPulsarMetadata(MANAGED_LEDGER)
                .map(mlName -> TopicName.get(TopicDomain.persistent.name() + "://" + mlName.replace("/persistent/", "/")))
                .or(() -> getPulsarMetadata(COMPACTED_TOPIC)
                        .map(TopicName::get)
                        .or(() -> getPulsarMetadata(SCHEMA_ID)
                                .map(schemaId -> TopicName.get(TopicDomain.persistent.name() + "://" + schemaId))));
    }

    public Optional<TopicName> getSchemaLedgerTopic() {
        return getPulsarMetadata(SCHEMA_ID)
                                .map(schemaId -> TopicName.get(TopicDomain.persistent.name() + "://" + schemaId));
    }

    public long getCtime() {
        return this.ledgerMetadata.getCtime();
    }

    public boolean isSchemaLedger() {
        return getPulsarMetadata(SCHEMA_ID).isPresent();
    }
    public boolean isCompactedTopic() {
        return getPulsarMetadata(COMPACTED_TOPIC).isPresent();
    }

    public boolean isManagedLedger() {
        return getPulsarMetadata(MANAGED_LEDGER).isPresent();
    }

    public boolean isPulsarLedger() {
        return "pulsar".equals(this.pulsarMetadata.get("application")) || this.pulsarMetadata.containsKey(MANAGED_CURSOR);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ledger that = (Ledger) o;
        return getLedgerId() == that.getLedgerId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLedgerId());
    }

    @Override
    public String toString() {
        return "Ledger " +
                getLedgerId() +
                " | ctime=" +
                getCtime() +
                " | topic=" +
                getLedgerTopic().orElse(null) +
                " | length=" +
                getLength() +
                " | schemaLedger=" +
                isSchemaLedger() +
                " | pulsarLedger=" +
                isPulsarLedger() +
                " | meta=" +
                pulsarMetadata;
    }
}
