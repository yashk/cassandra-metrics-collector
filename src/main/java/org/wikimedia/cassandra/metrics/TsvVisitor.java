package org.wikimedia.cassandra.metrics;


import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_PREFIX;

import java.io.PrintStream;


/**
 * Sample visitor that writes tab-seperated output to a {@link PrintStream}.
 * 
 * @author eevans
 */
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

    /** {@inheritDoc} */
    @Override
    public void visit(JmxSample jmxSample) {
        this.stream.printf(
                "%s %s %s%n",
                GraphiteVisitor.metricName(jmxSample, this.prefix),
                jmxSample.getValue(),
                jmxSample.getTimestamp());
    }

}
