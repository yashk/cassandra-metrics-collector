package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;

public class Sample {
    private final String name;
    private final Object value;
    private final Number timestamp;

    public Sample(String name, Object value, Number timestamp) {
        this.name = checkNotNull(name, "name argument");
        this.value = checkNotNull(value, "value argument");
        this.timestamp = checkNotNull(timestamp, "timestamp argument");
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public Number getTimestamp() {
        return timestamp;
    }

}