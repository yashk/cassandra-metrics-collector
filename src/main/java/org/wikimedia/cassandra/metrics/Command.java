package org.wikimedia.cassandra.metrics;

import java.io.IOException;

public class Command {
    public static void main(String... args) throws IOException {
        if (args.length != 5) {
            System.err.printf("Usage: %s <jmx host> <jmx port> <graphite host> <graphite port> <prefix>%n", Command.class.getSimpleName());
            System.exit(1);
        }

        Integer jmxPort = null, graphitePort = null;

        try {
            jmxPort = Integer.parseInt(args[1]);
        }
        catch (NumberFormatException e) {
            System.err.println("Not a valid port number: " + args[1]);
            System.exit(1);
        }

        try {
            graphitePort = Integer.parseInt(args[3]);
        }
        catch (NumberFormatException e) {
            System.err.println("Not a valid port number: " + args[3]);
            System.exit(1);
        }

        try (JmxCollector collector = new JmxCollector(args[0], jmxPort)) {
            if (Boolean.parseBoolean(System.getenv().get("DRY_RUN"))) {
                collector.getSamples(new TsvVisitor(System.out, args[4]));
            }
            else {
                try (GraphiteVisitor visitor = new GraphiteVisitor(args[2], graphitePort, args[4])) {
                    collector.getSamples(visitor);
                }
            }
        }

    }
}
