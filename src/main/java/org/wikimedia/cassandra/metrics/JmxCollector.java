package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.management.ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE;
import static java.lang.management.ManagementFactory.MEMORY_MXBEAN_NAME;
import static java.lang.management.ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE;
import static java.lang.management.ManagementFactory.RUNTIME_MXBEAN_NAME;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_JMX_HOST;
import static org.wikimedia.cassandra.metrics.Constants.DEFAULT_JMX_PORT;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
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

import org.wikimedia.cassandra.metrics.JmxSample.Type;

import com.google.common.collect.Sets;
import com.yammer.metrics.reporting.JmxReporter;


public class JmxCollector implements AutoCloseable {

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
        mbeanClasses.put("com.yammer.metrics.reporting.JmxReporter$Meter", JmxReporter.MeterMBean.class);

        blacklist = new HashSet<ObjectName>();
        blacklist.add(newObjectName("org.apache.cassandra.metrics:type=ColumnFamily,name=SnapshotsSize"));
        blacklist.add(newObjectName("org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=compactions_in_progress,name=SnapshotsSize"));

    }

    private final String hostname;
    private final int port;
    private final JMXConnector jmxc;
    private final MBeanServerConnection mbeanServerConn;
    private final ObjectName metricsObjectName;

    public JmxCollector() throws IOException {
        this(DEFAULT_JMX_HOST);
    }

    public JmxCollector(String host) throws IOException {
        this(host, DEFAULT_JMX_PORT);
    }

    public JmxCollector(String host, int port) throws IOException {
        this.hostname = checkNotNull(host, "host argument");
        this.port = checkNotNull(port, "port argument");

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
        RuntimeMXBean runtime = ManagementFactory.newPlatformMXBeanProxy(getConnection(), RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
        visitor.visit(new JmxSample(Type.JVM, newObjectName(RUNTIME_MXBEAN_NAME), "uptime", runtime.getUptime(), timestamp));

        // Memory
        MemoryMXBean memory = ManagementFactory.newPlatformMXBeanProxy(getConnection(), MEMORY_MXBEAN_NAME, MemoryMXBean.class);
        ObjectName oName = newObjectName(MEMORY_MXBEAN_NAME);
        visitor.visit(new JmxSample(Type.JVM, oName, "non_heap_usage", memory.getNonHeapMemoryUsage().getUsed(), timestamp));
        visitor.visit(new JmxSample(Type.JVM, oName, "heap_usage", memory.getHeapMemoryUsage().getUsed(), timestamp));

        // Garbage collection
        for (ObjectInstance instance : getConnection().queryMBeans(newObjectName("java.lang:type=GarbageCollector,name=*"), null)) {
            String name = instance.getObjectName().getKeyProperty("name");
            GarbageCollectorMXBean gc = newPlatformMXBeanProxy(GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE, "name", name, GarbageCollectorMXBean.class);
            visitor.visit(new JmxSample(Type.JVM, instance.getObjectName(), "runs", gc.getCollectionCount(), timestamp));
            visitor.visit(new JmxSample(Type.JVM, instance.getObjectName(), "time", gc.getCollectionTime(), timestamp));
        }

        // Memory pool usages
        for (ObjectInstance instance : getConnection().queryMBeans(newObjectName("java.lang:type=MemoryPool,name=*"), null)) {
            String name = instance.getObjectName().getKeyProperty("name");
            MemoryPoolMXBean memPool = newPlatformMXBeanProxy(MEMORY_POOL_MXBEAN_DOMAIN_TYPE, "name", name, MemoryPoolMXBean.class);
            visitor.visit(new JmxSample(Type.JVM, instance.getObjectName(), memPool.getName(), memPool.getUsage().getUsed(), timestamp));
        }

    }


    public void getCassandraSamples(SampleVisitor visitor) throws IOException {

        for (ObjectInstance instance : getConnection().queryMBeans(this.metricsObjectName, null)) {
            if (!interesting(instance.getObjectName()))
                continue;

            Object proxy = getMBeanProxy(instance);
            ObjectName oName = instance.getObjectName();

            int timestamp = (int) (System.currentTimeMillis() / 1000);

            if (proxy instanceof JmxReporter.MeterMBean) {
                JmxReporter.MeterMBean meter = (JmxReporter.MeterMBean)proxy;
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "15MinuteRate", meter.getFifteenMinuteRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "1MinuteRate", meter.getOneMinuteRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "5MinuteRate", meter.getFiveMinuteRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "count", meter.getCount(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "meanRate", meter.getMeanRate(), timestamp));
                continue;
            }

            if (proxy instanceof JmxReporter.TimerMBean) {
                JmxReporter.TimerMBean timer = (JmxReporter.TimerMBean)proxy;
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "50percentile", timer.get50thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "75percentile", timer.get75thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "95percentile", timer.get95thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "98percentile", timer.get98thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "99percentile", timer.get99thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "999percentile", timer.get999thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "1MinuteRate", timer.getOneMinuteRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "5MinuteRate", timer.getFiveMinuteRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "15MinuteRate", timer.getFifteenMinuteRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "count", timer.getCount(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "max", timer.getMax(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "mean", timer.getMean(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "meanRate", timer.getMeanRate(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "min", timer.getMin(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "stddev", timer.getStdDev(), timestamp));
                continue;
            }
            
            if (proxy instanceof JmxReporter.HistogramMBean) {
                JmxReporter.HistogramMBean histogram = (JmxReporter.HistogramMBean)proxy;
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "50percentile", histogram.get50thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "75percentile", histogram.get75thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "95percentile", histogram.get95thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "98percentile", histogram.get98thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "99percentile", histogram.get99thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "999percentile", histogram.get999thPercentile(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "max", histogram.getMax(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "mean", histogram.getMean(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "min", histogram.getMin(), timestamp));
                visitor.visit(new JmxSample(Type.CASSANDRA, oName, "stddev", histogram.getStdDev(), timestamp));
                continue;
            }

            if (proxy instanceof JmxReporter.GaugeMBean) {
                visitor.visit(new JmxSample(
                        Type.CASSANDRA,
                        oName,
                        "value",
                        ((JmxReporter.GaugeMBean) proxy).getValue(),
                        timestamp));
                continue;
            }

            if (proxy instanceof JmxReporter.CounterMBean) {
                visitor.visit(new JmxSample(
                        Type.CASSANDRA,
                        oName,
                        "count",
                        ((JmxReporter.CounterMBean) proxy).getCount(),
                        timestamp));
                continue;
            }
        }

    }

    @Override
    public void close() throws IOException {
        this.jmxc.close();
    }

    @Override
    public String toString() {
        return "JmxCollector [hostname="
                + hostname
                + ", port="
                + port
                + ", jmxc="
                + jmxc
                + ", mbeanServerConn="
                + mbeanServerConn
                + ", metricsObjectName="
                + metricsObjectName
                + "]";
    }

    MBeanServerConnection getConnection() {
        return this.mbeanServerConn;
    }

    Object getMBeanProxy(ObjectInstance instance) {
        return JMX.newMBeanProxy(getConnection(), instance.getObjectName(), mbeanClasses.get(instance.getClassName()));
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

        try (JmxCollector collector = new JmxCollector("localhost", 7100)) {
            SampleVisitor visitor = new SampleVisitor() {
                @Override
                public void visit(JmxSample jmxSample) {
                    System.err.printf("%s,%s=%s%n", jmxSample.getObjectName(), jmxSample.getMetricName(), jmxSample.getValue());
                }
            };
            collector.getSamples(visitor);
        }

    }

}
