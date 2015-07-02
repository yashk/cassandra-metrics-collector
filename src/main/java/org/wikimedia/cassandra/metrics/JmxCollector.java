package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.management.ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE;
import static java.lang.management.ManagementFactory.MEMORY_MXBEAN_NAME;
import static java.lang.management.ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.yammer.metrics.reporting.JmxReporter;

public class JmxCollector implements AutoCloseable {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 7199;
    private static final String DEFAULT_PREFIX = "cassandra";
    private static final String FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    private static final Map<String, Class<?>> mbeanClasses;
    private static final Set<ObjectName> blacklist;

    static {
        mbeanClasses = new HashMap<String, Class<?>>();
        mbeanClasses.put("com.yammer.metrics.reporting.JmxReporter$Gauge", JmxReporter.GaugeMBean.class);
        mbeanClasses.put("com.yammer.metrics.reporting.JmxReporter$Timer", JmxReporter.TimerMBean.class);
        mbeanClasses.put("com.yammer.metrics.reporting.JmxReporter$Counter", JmxReporter.CounterMBean.class);
        mbeanClasses.put("com.yammer.metrics.reporting.JmxReporter$Meter", JmxReporter.MeterMBean.class);
        mbeanClasses.put("com.yammer.metrics.reporting.JmxReporter$Histogram", JmxReporter.HistogramMBean.class);

        blacklist = new HashSet<ObjectName>();
        blacklist.add(newObjectName("org.apache.cassandra.metrics:type=ColumnFamily,name=SnapshotsSize"));
        blacklist.add(newObjectName("org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=compactions_in_progress,name=SnapshotsSize"));

    }

    private final String hostname;
    private final int port;
    private final String prefix;
    private final JMXConnector jmxc;
    private final MBeanServerConnection mbeanServerConn;
    private final ObjectName metricsObjectName;

    public JmxCollector() throws IOException {
        this(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_PREFIX);
    }

    public JmxCollector(String host, int port, String prefix) throws IOException {
        this.hostname = checkNotNull(host, "host argument");
        this.port = checkNotNull(port, "port argument");
        this.prefix = checkNotNull(prefix, "prefix argument");

        this.metricsObjectName = newObjectName("org.apache.cassandra.metrics:*");

        JMXServiceURL jmxUrl;
        try {
            jmxUrl = new JMXServiceURL(String.format(FORMAT_URL, this.hostname, this.port));
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        /* FIXME: add authentication support */
        Map<String, Object> env = new HashMap<String, Object>();
        this.jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        this.mbeanServerConn = jmxc.getMBeanServerConnection();
    }

    public void getSamples(SampleVisitor visitor) throws IOException {
        getJvmSamples(visitor);
        getCassandraSamples(visitor);
    }

    public void getJvmSamples(SampleVisitor visitor) throws IOException {
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        // Runtime
        RuntimeMXBean runtime = ManagementFactory.newPlatformMXBeanProxy(getConnection(), ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
        visitor.visit(new Sample(String.format("%s.jvm.uptime", this.prefix), runtime.getUptime(), timestamp));

        // Memory
        MemoryMXBean memory = ManagementFactory.newPlatformMXBeanProxy(getConnection(), MEMORY_MXBEAN_NAME, MemoryMXBean.class);
        visitor.visit(new Sample(this.prefix+".jvm.memory.non_heap_usage", memory.getNonHeapMemoryUsage().getUsed(), timestamp));
        visitor.visit(new Sample(this.prefix+".jvm.memory.heap_usage", memory.getHeapMemoryUsage().getUsed(), timestamp));

        // Garbage collection
        for (ObjectInstance instance : getConnection().queryMBeans(newObjectName("java.lang:type=GarbageCollector,name=*"), null)) {
            String name = instance.getObjectName().getKeyProperty("name");
            GarbageCollectorMXBean gc = newPlatformMXBeanProxy(GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE, "name", name, GarbageCollectorMXBean.class);
            /* FIXME: all names should be scrubbed like this. */
            visitor.visit(new Sample(String.format("%s.jvm.gc.%s.runs", this.prefix, gc.getName().replace(' ', '-')), gc.getCollectionCount(), timestamp));
            visitor.visit(new Sample(String.format("%s.jvm.gc.%s.time", this.prefix, gc.getName().replace(' ', '-')), gc.getCollectionTime(), timestamp));
        }

        // Memory pool usages
        for (ObjectInstance instance : getConnection().queryMBeans(newObjectName("java.lang:type=MemoryPool,name=*"), null)) {
            String name = instance.getObjectName().getKeyProperty("name");
            MemoryPoolMXBean memPool = newPlatformMXBeanProxy(MEMORY_POOL_MXBEAN_DOMAIN_TYPE, "name", name, MemoryPoolMXBean.class);
            visitor.visit(new Sample(String.format("%s.jvm.memory.memory_pool_usages.%s", this.prefix, memPool.getName().replace(' ', '-')), memPool.getUsage().getUsed(), timestamp));
        }

    }


    public void getCassandraSamples(SampleVisitor visitor) throws IOException {

        for (ObjectInstance instance : getConnection().queryMBeans(this.metricsObjectName, null)) {
            if (!interesting(instance.getObjectName()))
                continue;

            Joiner joiner = Joiner.on(".");
            Object proxy = getMBeanProxy(instance);

            String name = graphiteName(instance.getObjectName());
            int timestamp = (int) (System.currentTimeMillis() / 1000);

            if (proxy instanceof JmxReporter.TimerMBean) {
                visitor.visit(new Sample(joiner.join(name, "50percentile"), ((JmxReporter.TimerMBean) proxy).get50thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "75percentile"), ((JmxReporter.TimerMBean) proxy).get75thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "95percentile"), ((JmxReporter.TimerMBean) proxy).get95thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "98percentile"), ((JmxReporter.TimerMBean) proxy).get98thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "99percentile"), ((JmxReporter.TimerMBean) proxy).get99thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "999percentile"), ((JmxReporter.TimerMBean) proxy).get999thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "1MinuteRate"), ((JmxReporter.TimerMBean) proxy).getOneMinuteRate(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "5MinuteRate"), ((JmxReporter.TimerMBean) proxy).getFiveMinuteRate(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "15MinuteRate"), ((JmxReporter.TimerMBean) proxy).getFifteenMinuteRate(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "count"), ((JmxReporter.TimerMBean) proxy).getCount(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "max"), ((JmxReporter.TimerMBean) proxy).getMax(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "mean"), ((JmxReporter.TimerMBean) proxy).getMean(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "meanRate"), ((JmxReporter.TimerMBean) proxy).getMeanRate(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "min"), ((JmxReporter.TimerMBean) proxy).getMin(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "stddev"), ((JmxReporter.TimerMBean) proxy).getStdDev(), timestamp));
                continue;
            }
            
            if (proxy instanceof JmxReporter.HistogramMBean) {
                visitor.visit(new Sample(joiner.join(name, "50percentile"), ((JmxReporter.HistogramMBean) proxy).get50thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "75percentile"), ((JmxReporter.HistogramMBean) proxy).get75thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "95percentile"), ((JmxReporter.HistogramMBean) proxy).get95thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "98percentile"), ((JmxReporter.HistogramMBean) proxy).get98thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "99percentile"), ((JmxReporter.HistogramMBean) proxy).get99thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "999percentile"), ((JmxReporter.HistogramMBean) proxy).get999thPercentile(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "max"), ((JmxReporter.HistogramMBean) proxy).getMax(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "mean"), ((JmxReporter.HistogramMBean) proxy).getMean(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "min"), ((JmxReporter.HistogramMBean) proxy).getMin(), timestamp));
                visitor.visit(new Sample(joiner.join(name, "stddev"), ((JmxReporter.HistogramMBean) proxy).getStdDev(), timestamp));
                continue;
            }

            if (proxy instanceof JmxReporter.GaugeMBean) {
                visitor.visit(new Sample(joiner.join(name, "value"), ((JmxReporter.GaugeMBean) proxy).getValue(), timestamp));
                continue;
            }

            if (proxy instanceof JmxReporter.CounterMBean) {
                visitor.visit(new Sample(joiner.join(name, "count"), ((JmxReporter.CounterMBean) proxy).getCount(), timestamp));
                continue;
            }
        }

    }

    @Override
    public void close() throws IOException {
        this.jmxc.close();
    }

    MBeanServerConnection getConnection() {
        return this.mbeanServerConn;
    }

    Object getMBeanProxy(ObjectInstance instance) {
        return JMX.newMBeanProxy(getConnection(), instance.getObjectName(), mbeanClasses.get(instance.getClassName()));
    }

    String graphiteName(ObjectName name) {

        StringBuilder builder = new StringBuilder(this.prefix).append('.').append(name.getDomain());

        // Ideally we'd use getKeyPropertyList here, but that returns a map that
        // obscures the original ordering (which I assume/hope is stable), so
        // we're forced to parse it ourselves here.
        
        /* FIXME: you can actually hear this suck. */
        String propertiesString = name.getKeyPropertyListString();
        if (propertiesString.contains("type=ColumnFamily")) {
            if (name.getKeyProperty("keyspace") == null)
                propertiesString = propertiesString.replaceFirst("type=ColumnFamily", "type=ColumnFamily,keyspace=all");
        }
        for (String property : Splitter.on(",").trimResults().split(propertiesString)) {
            List<String> kv = Splitter.on("=").trimResults().limit(2).splitToList(property);
            builder.append('.').append(kv.get(1));
        }

        return builder.toString();
    }

    /* TODO: Ideally, the "interesting" criteria should be configurable. */
    private static Set<String> interestingTypes = Sets.newHashSet(
            "Cache",
            "ClientRequest",
            "ColumnFamily",
            "Connection",
            "CQL",
            "DroppedMessage",
            "FileCache",
            "IndexColumnFamily",
            "Storage",
            "ThreadPools",
            "Compaction",
            "ReadRepair",
            "CommitLog");

    /* XXX: This is a hot mess. */
    private boolean interesting(ObjectName objName) {
        if (blacklist.contains(objName))
            return false;

        /* XXX: These metrics are gauges that return long[]; Pass for now... */
        String name = objName.getKeyProperty("name");
        if (name != null && (name.equals("EstimatedRowSizeHistogram") || name.equals("EstimatedColumnCountHistogram"))) {
            return false;
        }

        String type = objName.getKeyProperty("type");
        if (type != null && interestingTypes.contains(type)) {
            String keyspace = objName.getKeyProperty("keyspace");
            if (keyspace == null || !keyspace.startsWith("system"))
                return true;
        }

        return false;
    }

    private <T> T newPlatformMXBeanProxy(String domainType, String key, String val, Class<T> cls) throws IOException {
        return ManagementFactory.newPlatformMXBeanProxy(getConnection(), String.format("%s,%s=%s", domainType, key, val), cls); 
    }

    /**
     * An {@link ObjectName} factory that throws unchecked exceptions for a malformed name.  This is a convenience method
     * to avoid exception handling for {@link ObjectName} instantiation with constants.
     * 
     * @param name and object name
     * @return the ObjectName instance corresponding to name
     */
    private static ObjectName newObjectName(String name) {
        try {
            return new ObjectName(name);
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException("a bug!", e);
        }
    }

    public static void main(String... args) throws IOException, Exception {

        try (JmxCollector collector = new JmxCollector("localhost", 7100, "cassandra")) {
            SampleVisitor visitor = new SampleVisitor() {
                @Override
                public void visit(Sample sample) {
                    System.out.printf("%s=%s%n", sample.getName(), sample.getValue());
                }
            };
            collector.getJvmSamples(visitor);
        }

    }

}
