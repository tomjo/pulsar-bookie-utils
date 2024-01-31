# pulsar-bookie-utils

A command-line utility to perform various maintenance tasks for Apache [Pulsar](https://pulsar.apache.org/) / [BookKeeper](https://bookkeeper.apache.org/).

## Context

During working with Pulsar and BookKeeper, I've encountered various issues that required some manual intervention, usually these were the result of bugs or infrastructure failures. 
This tool is a collection of various commands based on initially one-off code that I've used to solve these issues. 
During normal operation these shouldn't be used and I recommend you get familiar with Pulsar and BookKeeper before using this tool.

In my context this was only used with ZooKeeper as metadata store, but the code should be pretty adaptable to other metadata stores.

Does not support storage offloading as this was not used in my context.

I also used this as an opportunity to play around with functional programming in java using [vavr](https://github.com/vavr-io/vavr).

## Usage

```shell
java -jar pulsar-bookie-utils.jar <command> [options]
```

```shell
Usage: pulsar-bookie-utils [-hV] [COMMAND] <OPTIONS>
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  clean-orphan-ledgers    Cleans up 'orphan' ledgers (ledgers in BookKeeper but
                            not in ZooKeeper). Minimal age to be considered
  deep-clean              Deletes all data associated with topic (topic,
                            ledgers, cursors, ...) from BookKeeper and
                            ZooKeeper.
  detect-missing-ledgers  Detects missing ledgers associated with
                            topic/namespace/tenant.
  get-storage-size        Get aggregated storage size of tenant, namespace or
                            topic
  load-inactive-topics    Load inactive topics older than threshold for
                            namespace - this can be used to trigger their
                            retention policy, triggering cleanup
  trim-ledgers            Trim the oldest existing ledgers to free up space,
                            either by amount of ledgers or by date. By default
                            only considers expired ledgers.
```
 ### clean-orphan-ledgers

```
Cleans up 'orphan' ledgers (ledgers in BookKeeper but not in ZooKeeper). Minimal age to be considered orphaned is configurable.

Options:
      --auth-params=<authParams>                          Pulsar auth params
      --auth-plugin=<authPlugin>                          Pulsar auth plugin
  -d, --dry-run                                           Only log the ledgers eligible for cleanup, don't actually delete them
      --min-orphan-age=<minimumOrphanAge>                 Minimum orphan ledger age in days. Default 10 days
  -p, --pulsar-admin=<pulsarAdminHost>                    Pulsar admin endpoint
      --tls-trust-certs-file-path=<tlsTrustCertsFilePath> Path to certificate to be trusted for TLS connection
  -z, --zookeeper=<zookeeperHost>                         Zookeeper host
 -zt, --zookeeper-timeout=<zookeeperTimeout>              Zookeeper session timeout in milliseconds
```

### deep-clean

```
Deletes all data associated with topic (topic, ledgers, cursors, ...) from BookKeeper and ZooKeeper.

Arguments:
      <resource>                                          The resource to deep clean.

Options:
      --auth-params=<authParams>                          Pulsar auth params
      --auth-plugin=<authPlugin>                          Pulsar auth plugin
  -d, --dry-run                                           Only log the resources to be cleaned
  -f, --force                                             Force clean all resources detected
  -p, --pulsar-admin=<pulsarAdminHost>                    Pulsar admin endpoint
      --tls-trust-certs-file-path=<tlsTrustCertsFilePath> Path to certificate to be trusted for TLS connection
  -z, --zookeeper=<zookeeperHost>                         Zookeeper host
 -zt, --zookeeper-timeout=<zookeeperTimeout>              Zookeeper session timeout in milliseconds
```

### detect-missing-ledgers

```
Detects missing ledgers associated with topic/namespace/tenant.

Arguments:
      <resource>                                          The resource to detect missing ledgers for.

Options:
      --auth-params=<authParams>                          Pulsar auth params
      --auth-plugin=<authPlugin>                          Pulsar auth plugin
  -p, --pulsar-admin=<pulsarAdminHost>                    Pulsar admin endpoint
      --tls-trust-certs-file-path=<tlsTrustCertsFilePath> Path to certificate to be trusted for TLS connection
  -z, --zookeeper=<zookeeperHost>                         Zookeeper host
 -zt, --zookeeper-timeout=<zookeeperTimeout>              Zookeeper session timeout in milliseconds
```

### get-storage-size

```
Get aggregated storage size of tenant, namespace or topic

Arguments:
      <resource>                                          The resource whose storage size to get.

Options:
      --auth-params=<authParams>                          Pulsar auth params
      --auth-plugin=<authPlugin>                          Pulsar auth plugin
  -p, --pulsar-admin=<pulsarAdminHost>                    Pulsar admin endpoint
      --tls-trust-certs-file-path=<tlsTrustCertsFilePath> Path to certificate to be trusted for TLS connection
```

### load-inactive-topics

```
Load inactive topics older than threshold for namespace - this can be used to trigger their retention policy, triggering cleanup

Arguments:
      <namespace>                                         The namespace whose inactive topics to load.

Options:
      --auth-params=<authParams>                          Pulsar auth params
      --auth-plugin=<authPlugin>                          Pulsar auth plugin
  -d, --dry-run                                           Only log the eligible inactive topics
      --inactive-days=<inactiveDaysThreshold>             Minimum days inactive. Default 10 days
  -p, --pulsar-admin=<pulsarAdminHost>                    Pulsar admin endpoint
      --tls-trust-certs-file-path=<tlsTrustCertsFilePath> Path to certificate to be trusted for TLS connection
  -z, --zookeeper=<zookeeperHost>                         Zookeeper host
 -zt, --zookeeper-timeout=<zookeeperTimeout>              Zookeeper session timeout in milliseconds
```

### trim-ledgers

```
Trim the oldest existing ledgers to free up space, either by amount of ledgers or by date. By default only considers expired ledgers.

Options:
      --auth-params=<authParams>                          Pulsar auth params
      --auth-plugin=<authPlugin>                          Pulsar auth plugin
  -b, --before-date=<trimBeforeDate>                      Amount of ledgers to trim. Default 10
  -d, --dry-run                                           Only log the ledgers eligible for trimming, don't actually delete them
  -f, --force                                             Force trim ledgers even though not expired according to retention policies
  -n, --amount=<amount>                                   Amount of ledgers to trim. Default 10
  -p, --pulsar-admin=<pulsarAdminHost>                    Pulsar admin endpoint
      --tls-trust-certs-file-path=<tlsTrustCertsFilePath> Path to certificate to be trusted for TLS connection
  -z, --zookeeper=<zookeeperHost>                         Zookeeper host
 -zt, --zookeeper-timeout=<zookeeperTimeout>              Zookeeper session timeout in milliseconds
```

## Installation

### Pre-built jar

You can download the pre-built jar file from the latest GitHub release: https://github.com/tomjo/pulsar-bookie-utils/releases/latest

### Container image

Images available at https://ghcr.io/tomjo/pulsar-bookie-utils

### Building from source

Requires Java 11 (or later).

```shell
./gradlew build
```

The container build requires a container build tool that handles Dockerfile such as Docker, Podman, Kaniko, ...

## License

Licensed as **Apache-2.0**. See LICENSE file for details. If you have a use case that would require a different license, please contact me.
