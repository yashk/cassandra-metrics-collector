package org.wikimedia.cassandra.metrics;

import java.io.IOException;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;

public class DiscoverCommand {

    private static String prefix(String id) {
        return String.format("cassandra.%s", id);
    }

    public static void main(String... args) throws IOException, AttachNotSupportedException, AgentLoadException, AgentInitializationException {
        if (args.length != 2) {
            System.err.printf("Usage: %s <graphite host> <graphite port>%n", Command.class.getSimpleName());
            System.exit(1);
        }

        Integer graphitePort = null;

        try {
            graphitePort = Integer.parseInt(args[1]);
        }
        catch (NumberFormatException e) {
            System.err.println("Not a valid port number: " + args[1]);
            System.exit(1);
        }

        for (Discovery.Jvm jvm : new Discovery().getJvms().values()) {
            try (JmxCollector collector = new JmxCollector(jvm.getJmxUrl())) {
                if (Boolean.parseBoolean(System.getenv().get("DRY_RUN"))) {
                    collector.getSamples(new TsvVisitor(System.out, prefix(jvm.getCassandraInstance())));
                }
                else {
                    try (GraphiteVisitor visitor = new GraphiteVisitor(args[0], graphitePort, prefix(jvm.getCassandraInstance()))) {
                        collector.getSamples(visitor);
                    }
                }
            }
        }
    }

}
