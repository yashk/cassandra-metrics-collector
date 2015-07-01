package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
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
        try {
            blacklist.add(new ObjectName("org.apache.cassandra.metrics:type=ColumnFamily,name=SnapshotsSize"));
            blacklist
                    .add(new ObjectName(
                            "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=compactions_in_progress,name=SnapshotsSize"));
        }
        catch (MalformedObjectNameException e) {
            e.printStackTrace(); /* FIXME: handle this. */
        }
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

        try {
            this.metricsObjectName = new ObjectName("org.apache.cassandra.metrics:*");
        }
        catch (MalformedObjectNameException e1) {
            throw new RuntimeException("a bug!");
        }

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

        for (ObjectInstance instance : getConnection().queryMBeans(this.metricsObjectName, null)) {
            if (blacklist.contains(instance.getObjectName()))
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

    MBeanServerConnection getConnection() {
        return this.mbeanServerConn;
    }

    Object getMBeanProxy(ObjectInstance instance) {
        return JMX.newMBeanProxy(getConnection(), instance.getObjectName(), mbeanClasses.get(instance.getClassName()));
    }

    String graphiteName(ObjectName name) {

        StringBuilder builder = new StringBuilder(this.prefix).append('.').append(this.hostname).append('.')
                .append(name.getDomain());

        // Ideally we'd use getKeyPropertyList here, but that returns a map that
        // obscures the original ordering (which I assume/hope is stable), so
        // we're forced to parse it ourselves here.
        for (String property : Splitter.on(",").trimResults().split(name.getKeyPropertyListString())) {
            List<String> kv = Splitter.on("=").trimResults().limit(2).splitToList(property);
            builder.append('.').append(kv.get(1));
        }

        return builder.toString();
    }

    @Override
    public void close() throws IOException {
        this.jmxc.close();
    }

    public static void main(String... args) throws IOException, Exception {

        try (JmxCollector collector = new JmxCollector("localhost", 7100, "cassandra")) {
            SampleVisitor visitor = new SampleVisitor() {
                @Override
                public void visit(Sample sample) {
                    System.out.printf("%s=%s%n", sample.getName(), sample.getValue());
                }
            };
            collector.getSamples(visitor);
        }

    }

}
