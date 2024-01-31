package net.tomjo.pulsarbookieutils;

import io.quarkus.arc.DefaultBean;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import net.tomjo.pulsarbookieutils.command.EntryCommand;
import picocli.CommandLine;

import java.time.Clock;

@QuarkusMain
public class PulsarBookieUtils implements QuarkusApplication {

    @Inject
    CommandLine.IFactory factory;

    @Produces
    @DefaultBean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Override
    public int run(String... args) {
        return new CommandLine(new EntryCommand(), factory).execute(args);
    }
}
