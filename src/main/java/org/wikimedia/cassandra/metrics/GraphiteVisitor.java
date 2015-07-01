package org.wikimedia.cassandra.metrics;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;


public class GraphiteVisitor implements SampleVisitor, AutoCloseable {
    private final String hostname;
    private final int port;

    private Socket socket;
    private OutputStream outStream;
    private PrintWriter writer;
    private boolean isClosed = false;

    public GraphiteVisitor(String host, int port) {
        this.hostname = checkNotNull(host, "host argument");
        this.port = checkNotNull(port, "port argument");

        try {
            this.socket = createSocket();
            this.outStream = socket.getOutputStream();
        }
        catch (IOException e) {
            throw new GraphiteException(String.format("error connecting to %s:%d", this.hostname, this.port), e);
        }

        this.writer = new PrintWriter(this.outStream, true);
    }

    @Override
    public void visit(Sample sample) {
        if (this.isClosed) throw new GraphiteException("cannot call visit on closed object");
        this.writer.println(String.format("%s %s %d", sample.getName(), sample.getValue(), sample.getTimestamp()));
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
        this.outStream.close();
        this.socket.close();
        this.isClosed = true;
    }

    protected Socket createSocket() throws UnknownHostException, IOException {
        return new Socket(this.hostname, this.port);
    }

}
