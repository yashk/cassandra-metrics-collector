package org.wikimedia.cassandra.metrics;

public class GraphiteException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public GraphiteException(String msg) {
        super(msg);
    }

    public GraphiteException(Throwable cause) {
        super(cause);
    }

    public GraphiteException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
