package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_HOST;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_PORT;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_PREFIX;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.management.ObjectName;

import com.google.common.base.Splitter;


public class GraphiteVisitor implements SampleVisitor, AutoCloseable {
    private final String hostname;
    private final int port;
    private final String prefix;

    private Socket socket;
    private OutputStream outStream;
    private PrintWriter writer;
    private boolean isClosed = false;

    public GraphiteVisitor() {
        this(DEFAULT_GRAPHITE_HOST, DEFAULT_GRAPHITE_PORT);
    }

    public GraphiteVisitor(String host, int port) {
        this(host, port, DEFAULT_GRAPHITE_PREFIX);
    }

    public GraphiteVisitor(String host, int port, String prefix) {
        this.hostname = checkNotNull(host, "host argument");
        this.port = checkNotNull(port, "port argument");
        this.prefix = checkNotNull(prefix, "prefix argument");

        try {
            this.socket = createSocket();
            this.outStream = socket.getOutputStream();
        }
        catch (IOException e) {
            throw new GraphiteException(String.format("error connecting to %s:%d", this.hostname, this.port), e);
        }

        this.writer = new PrintWriter(this.outStream, true);
    }

    @Override
    public void visit(Sample sample) {
        if (this.isClosed) throw new GraphiteException("cannot call visit on closed object");
        this.writer.println(String.format("%s %s %d", metricName(sample, this.prefix), sample.getValue(), sample.getTimestamp()));
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
        this.outStream.close();
        this.socket.close();
        this.isClosed = true;
    }

    protected Socket createSocket() throws UnknownHostException, IOException {
        return new Socket(this.hostname, this.port);
    }

    static String metricName(Sample sample, String prefix) {
        switch (sample.getType()) {
            case JVM:
                return metricNameJvm(sample, prefix);
            case CASSANDRA:
                return metricNameCassandra(sample, prefix);
            default:
                throw new IllegalArgumentException("unknown sample type; report this as a bug!");
        }
    }

    static String metricNameJvm(Sample sample, String prefix) {
        ObjectName oName = sample.getObjectName();

        // Validate
        if (!oName.getDomain().equals("java.lang")) {
            throw new IllegalArgumentException("not a valid jvm sample");
        }

        String type = oName.getKeyProperty("type");

        if (type == null) {
            throw new IllegalArgumentException("resource w/o type");
        }

        // Create graphite metric name
        String baseName = String.format("%s.jvm", prefix);
        String metricName = scrub(sample.getMetricName());

        switch (type) {
            case "Runtime":
                return String.format("%s.%s", baseName, metricName);
            case "Memory":
                return String.format("%s.memory.%s", baseName, metricName);
            case "GarbageCollector":
                String gcName = sample.getObjectName().getKeyProperty("name");
                return String.format("%s.gc.%s.%s", baseName, scrub(gcName), metricName);
            case "MemoryPool":
                return String.format("%s.memory.memory_pool_usages.%s", baseName, metricName);
            default:
                throw new IllegalArgumentException("unknown bean type; report this as a bug!");
        }

    }

    static String metricNameCassandra(Sample sample, String prefix) {
        ObjectName oName = sample.getObjectName();
        StringBuilder builder = new StringBuilder(prefix).append('.').append(oName.getDomain());

        // Ideally we'd use getKeyPropertyList here, but that returns a map that
        // obscures the original ordering (which I assume/hope is stable), so
        // we're forced to parse it ourselves here.

        /* FIXME: you can actually hear this suck. */
        String propertiesString = oName.getKeyPropertyListString();
        if (propertiesString.contains("type=ColumnFamily")) {
            if (oName.getKeyProperty("keyspace") == null)
                propertiesString = propertiesString.replaceFirst("type=ColumnFamily", "type=ColumnFamily,keyspace=all");
        }
        for (String property : Splitter.on(",").trimResults().split(propertiesString)) {
            List<String> kv = Splitter.on("=").trimResults().limit(2).splitToList(property);
            builder.append('.').append(kv.get(1));
        }

        builder.append('.').append(sample.getMetricName());

        return builder.toString();
    }

    private static String scrub(String name) {
        return name.replace(' ', '-');
    }

}
