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


/**
 * JmxSample visitor implementation that writes metrics to Graphite. The aim is to be compatible with
 * Dropwizards GraphiteReporter persistence.
 * 
 * @author eevans
 */
public class GraphiteVisitor implements SampleVisitor, AutoCloseable {
    private final String hostname;
    private final int port;
    private final String prefix;

    private Socket socket;
    private OutputStream outStream;
    private PrintWriter writer;
    private boolean isClosed = false;

    /**
     * Create a new {@link GraphiteVisitor} using the default host, port, and prefix.
     */
    public GraphiteVisitor() {
        this(DEFAULT_GRAPHITE_HOST);
    }

    /**
     * Create a new {@link GraphiteVisitor} with the supplied host, and the default port and prefix.
     * 
     * @param host
     *            the Graphite host to connect to
     */
    public GraphiteVisitor(String host) {
        this(host, DEFAULT_GRAPHITE_PORT);
    }

    /**
     * Create a new {@link GraphiteVisitor} with the supplied host and port, and the default prefix.
     * 
     * @param host
     *            Graphite host to connect to
     * @param port
     *            the Graphite port number
     */
    public GraphiteVisitor(String host, int port) {
        this(host, port, DEFAULT_GRAPHITE_PREFIX);
    }

    /**
     * Create a new {@link GraphiteVisitor} using the supplied host, port, and prefix.
     * 
     * @param host
     *            Graphite host to connect to
     * @param port
     *            the Graphite port number
     * @param prefix
     *            string to prefix to each metric name
     */
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

    /** {@inheritDoc} */
    @Override
    public void visit(JmxSample jmxSample) {
        if (this.isClosed) throw new GraphiteException("cannot call visit on closed object");
        this.writer.println(String.format("%s %s %d", metricName(jmxSample, this.prefix), jmxSample.getValue(), jmxSample.getTimestamp()));
    }

    /**
     * Terminates the connection to Graphite. Once closed, this object is no longer valid; A new
     * instance will need to be created to send additional samples.
     */
    @Override
    public void close() throws IOException {
        this.writer.flush();
        this.writer.close();
        this.outStream.close();
        this.socket.close();
        this.isClosed = true;
    }

    @Override
    public String toString() {
        return "GraphiteVisitor [hostname="
                + hostname
                + ", port="
                + port
                + ", prefix="
                + prefix
                + ", socket="
                + socket
                + ", outStream="
                + outStream
                + ", writer="
                + writer
                + ", isClosed="
                + isClosed
                + "]";
    }

    protected Socket createSocket() throws UnknownHostException, IOException {
        return new Socket(this.hostname, this.port);
    }

    // There doesn't seem to be a deterministic way to translate these JMX resources to Graphite
    // names in an abstract way, thus all of the special-case handling that follows. :(

    static String metricName(JmxSample jmxSample, String prefix) {
        switch (jmxSample.getType()) {
            case JVM:
                return metricNameJvm(jmxSample, prefix);
            case CASSANDRA:
                return metricNameCassandra(jmxSample, prefix);
            default:
                throw new IllegalArgumentException("unknown sample type; report this as a bug!");
        }
    }

    static String metricNameJvm(JmxSample jmxSample, String prefix) {
        ObjectName oName = jmxSample.getObjectName();
        validateObjectName(oName, "java.lang", true);

        String type = oName.getKeyProperty("type");

        // Create graphite metric name
        String baseName = String.format("%s.jvm", prefix);
        String metricName = scrub(jmxSample.getMetricName());

        switch (type) {
            case "Runtime":
                return String.format("%s.%s", baseName, metricName);
            case "Memory":
                return String.format("%s.memory.%s", baseName, metricName);
            case "GarbageCollector":
                String gcName = jmxSample.getObjectName().getKeyProperty("name");
                return String.format("%s.gc.%s.%s", baseName, scrub(gcName), metricName);
            case "MemoryPool":
                return String.format("%s.memory.memory_pool_usages.%s", baseName, metricName);
            default:
                throw new IllegalArgumentException("unknown bean type; report this as a bug!");
        }

    }

    static String metricNameCassandra(JmxSample jmxSample, String prefix) {
        ObjectName oName = jmxSample.getObjectName();
        validateObjectName(oName, "org.apache.cassandra.metrics", true);

        StringBuilder builder = new StringBuilder(prefix).append('.').append(oName.getDomain());

        // Ideally we'd be able to build a deterministic metric name by iterating
        // over the already parsed property list, but as it's a hash map, the
        // ordering is lost, and we're forced to resort to string manipulation.

        String propsString = oName.getKeyPropertyListString();

        // If type=ColumnFamily, but keyspace=null, then add this metric to a special "all"
        // keyspace. This is what Dropwizard's GraphiteReporter does, and we aim to be compatible.
        if (oName.getKeyProperty("type").equals("ColumnFamily")) {
            if (oName.getKeyProperty("keyspace") == null) {
                propsString = propsString.replaceFirst("type=ColumnFamily", "type=ColumnFamily,keyspace=all");
            }
        }

        // Add the remaining properties in the order they were defined.
        for (String property : Splitter.on(",").trimResults().split(propsString)) {
            List<String> kv = Splitter.on("=").trimResults().limit(2).splitToList(property);
            builder.append('.').append(scrub(kv.get(1)));
        }

        builder.append('.').append(scrub(jmxSample.getMetricName()));

        return builder.toString();
    }

    /** scrub problematic characters */
    private static String scrub(String name) {
        return name.replace(' ', '-');
    }

    private static void validateObjectName(ObjectName oName, String expectedDomain, boolean typeRequired) {
        checkNotNull(oName, "oName argument");
        checkNotNull(expectedDomain, "expectedDomain argument");

        // validate the expected domain name
        if (!oName.getDomain().equals(expectedDomain))
            throw new IllegalArgumentException(String.format("sample not in domain %s", expectedDomain));

        if (typeRequired && oName.getKeyProperty("type") == null)
            throw new IllegalArgumentException("missing type property");
    }

}
