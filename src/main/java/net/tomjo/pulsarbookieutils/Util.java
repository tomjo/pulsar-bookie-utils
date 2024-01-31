package net.tomjo.pulsarbookieutils;

import io.vavr.control.Try;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

public class Util {

    public static boolean isTopic(String identifier) {
        return Try.of(() -> TopicName.get(identifier))
                .map(t -> true)
                .recover(e -> false)
                .get();
    }

    public static boolean isNamespace(String identifier) {
        return !isTopic(identifier)
                && Try.of(() -> NamespaceName.get(identifier))
                .map(t -> true)
                .recover(e -> false)
                .get();
    }

}
