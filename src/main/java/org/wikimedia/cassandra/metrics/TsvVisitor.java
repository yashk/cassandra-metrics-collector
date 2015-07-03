package org.wikimedia.cassandra.metrics;


import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_PREFIX;

import java.io.PrintStream;


public class TsvVisitor implements SampleVisitor {

    private final PrintStream stream;
    private final String prefix;

    public TsvVisitor() {
        this(System.out, DEFAULT_GRAPHITE_PREFIX);
    }

    public TsvVisitor(PrintStream stream, String prefix) {
        this.stream = stream;
        this.prefix = prefix;
    }

    @Override
    public void visit(Sample sample) {
        this.stream.printf(
                "%s %s %s%n",
                GraphiteVisitor.metricName(sample, this.prefix),
                sample.getValue(),
                sample.getTimestamp());
    }

}
