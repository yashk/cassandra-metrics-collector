package org.wikimedia.cassandra.metrics;


import static com.google.common.base.Preconditions.checkNotNull;

import javax.management.ObjectName;


public class Sample {

    public static enum Type {
        JVM, CASSANDRA;
    }

    private final Type type;
    private final ObjectName objectName;
    private final String name;
    private final Object value;
    private final Number timestamp;

    public Sample(Type type, ObjectName oName, String metricName, Object value, Number timestamp) {
        this.type = checkNotNull(type, "type argument");
        this.objectName = checkNotNull(oName, "objectName argument");
        this.name = checkNotNull(metricName, "name argument");
        this.value = checkNotNull(value, "value argument");
        this.timestamp = checkNotNull(timestamp, "timestamp argument");
    }

    public Type getType() {
        return this.type;
    }

    public ObjectName getObjectName() {
        return this.objectName;
    }

    public String getMetricName() {
        return this.name;
    }

    public Object getValue() {
        return this.value;
    }

    public Number getTimestamp() {
        return this.timestamp;
    }

}