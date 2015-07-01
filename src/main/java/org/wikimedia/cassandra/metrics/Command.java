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

        try (JmxCollector collector = new JmxCollector(args[0], jmxPort, args[4])) {
            if (Boolean.parseBoolean(System.getenv().get("DRY_RUN"))) {
                collector.getSamples(new SampleVisitor() {
                    @Override
                    public void visit(Sample sample) {
                        System.out.printf("%s %s %s%n", sample.getName(), sample.getValue(), sample.getTimestamp());
                    }
                });
            }
            else {
                try (GraphiteVisitor visitor = new GraphiteVisitor(args[2], graphitePort)) {
                    collector.getSamples(visitor);
                }
            }
        }

    }
}
