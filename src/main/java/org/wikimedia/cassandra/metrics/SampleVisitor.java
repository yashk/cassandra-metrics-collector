package org.wikimedia.cassandra.metrics;


/**
 * JmxSample visitor interface.
 * <p>
 * Implementations of this interface can be passed to {@link JmxCollector#getSamples(SampleVisitor)}
 * , and be passed {@link JmxSample}s collected.
 * </p>
 * 
 * @author eevans
 */
public interface SampleVisitor {

    /**
     * Visit a sample; Each sample collected will be passed to this method for processing.
     * 
     * @param sample
     *            a collected sample
     */
    public void visit(JmxSample sample);
}
