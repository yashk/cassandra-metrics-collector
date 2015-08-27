/* Copyright 2015 Eric Evans <eevans@wikimedia.org>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_HOST;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_PORT;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_GRAPHITE_PREFIX;

import java.io.IOException;
import java.util.List;

import javax.management.ObjectName;

import com.google.common.base.Splitter;


/**
 * JmxSample visitor implementation that writes metrics to Graphite. The aim is to be compatible with
 * Dropwizard's GraphiteReporter persistence.
 * 
 * @author eevans
 */
public class CarbonVisitor implements SampleVisitor, AutoCloseable {
    private final String prefix;

    private CarbonConnector connector;
    private boolean isClosed = false;

    /**
     * Create a new {@link CarbonVisitor} using the default host, port, and
     * prefix.
     */
    public CarbonVisitor() {
        this(DEFAULT_GRAPHITE_HOST);
    }

    /**
     * Create a new {@link CarbonVisitor} with the supplied host, and the
     * default port and prefix.
     * 
     * @param host
     *            the Graphite host to connect to
     */
    public CarbonVisitor(String host) {
        this(host, DEFAULT_GRAPHITE_PORT);
    }

    /**
     * Create a new {@link CarbonVisitor} with the supplied host and port, and
     * the default prefix.
     * 
     * @param host
     *            Graphite host to connect to
     * @param port
     *            the Graphite port number
     */
    public CarbonVisitor(String host, int port) {
        this(host, port, DEFAULT_GRAPHITE_PREFIX);
    }

    /**
     * Create a new {@link CarbonVisitor} using the supplied host, port, and
     * prefix.
     * 
     * @param host
     *            Graphite host to connect to
     * @param port
     *            the Graphite port number
     * @param prefix
     *            string to prefix to each metric name
     */
    public CarbonVisitor(String host, int port, String prefix) {
        checkNotNull(host, "host argument");
        checkNotNull(port, "port argument");
        this.prefix = checkNotNull(prefix, "prefix argument");

        this.connector = new CarbonConnector(host, port);

    }

    /**
     * Create a new {@link CarbonVisitor} using the supplied {@link CarbonConnector}.
     * 
     * @param carbon
     *            Carbon connector instance
     * @param prefix
     *            string to prefix to each metric name
     */
    public CarbonVisitor(CarbonConnector carbon, String prefix) {
        this.connector = checkNotNull(carbon, "carbon argument");
        this.prefix = checkNotNull(prefix, "prefix argument");
    }
    
    /** {@inheritDoc} */
    @Override
    public void visit(JmxSample jmxSample) {
        checkState(!this.isClosed, "cannot write to closed object");
        this.connector.write(metricName(jmxSample, this.prefix), jmxSample.getValue(), jmxSample.getTimestamp());
    }

    /**
     * Terminates the connection to Graphite. Once closed, this object is no longer valid; A new
     * instance will need to be created to send additional samples.
     */
    @Override
    public void close() throws IOException {
        this.connector.close();
        this.isClosed = true;
    }

    @Override
    public String toString() {
        return "CarbonVisitor [carbonConnector=" + connector + ", prefix=" + prefix + ", isClosed=" + isClosed + "]";
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
