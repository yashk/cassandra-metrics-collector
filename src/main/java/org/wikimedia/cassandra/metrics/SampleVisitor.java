package org.wikimedia.cassandra.metrics;

public interface SampleVisitor {
    public void visit(Sample sample);
}
