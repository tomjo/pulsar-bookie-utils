package net.tomjo.pulsarbookieutils.command;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import picocli.CommandLine;

@TopCommand
@CommandLine.Command(name = "pulsar-bookie-utils", mixinStandardHelpOptions = true, subcommands = {CleanOrphanLedgersCommand.class, DeepCleanCommand.class, DetectMissingLedgersCommand.class, GetStorageSizeCommand.class, LoadInactiveTopicsCommand.class, TrimLedgersCommand.class})
public class EntryCommand {
}
