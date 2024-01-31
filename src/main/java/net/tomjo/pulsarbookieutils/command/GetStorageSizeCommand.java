package net.tomjo.pulsarbookieutils.command;

import io.quarkus.logging.Log;
import io.vavr.control.Try;
import net.tomjo.pulsarbookieutils.service.PulsarResourcesService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.tomjo.pulsarbookieutils.service.ServiceFactoryMethods.createPulsarResourcesService;

@Command(name = "get-storage-size", description = "Get aggregated storage size of tenant, namespace or topic")
public class GetStorageSizeCommand implements Runnable {

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

    @Parameters(index = "0", description = "The resource whose storage size to get.")
    String resource;

    private PulsarResourcesService pulsarResourcesService;

    public GetStorageSizeCommand() {
    }

    @Override
    public void run() {
        Map<String, Object> pulsarConfig = new HashMap<>();
        if (tlsTrustCertsFilePath != null) {
            pulsarConfig.put("tlsTrustCertsFilePath", tlsTrustCertsFilePath);
        }
        try {
            this.pulsarResourcesService = createPulsarResourcesService(pulsarAdminHost, authPlugin, authParams, pulsarConfig).get();
            printStorageSize();
        } finally {
            Try.run(this.pulsarResourcesService::close);
        }
    }

    private void printStorageSize() {
        List<String> topics = pulsarResourcesService.listTopics(resource);
        long storageSize = topics.stream()
                .mapToLong(topic -> Try.of(() -> getStorageSize(topic))
                        .recover(this::countInaccessibleSizeAsZero)
                        .get())
                .sum();
        Log.info("Storage size for " + resource + ": " + storageSize + " bytes");
    }

    private long countInaccessibleSizeAsZero(Throwable e) {
        Log.error("Error getting storage size for topic " + resource + ", counting it as 0: " + e.getMessage(), e);
        return 0L;
    }

    private long getStorageSize(String topic) throws PulsarAdminException {
        return pulsarResourcesService.topics().getStats(topic).getStorageSize();
    }

}
