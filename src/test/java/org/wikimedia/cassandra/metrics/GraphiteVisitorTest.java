package org.wikimedia.cassandra.metrics;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.wikimedia.cassandra.metrics.JmxSample.Type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Test;


public class GraphiteVisitorTest {

    @Test
    public void test() throws IOException, MalformedObjectNameException {
        final Socket socket = mock(Socket.class);
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        when(socket.getOutputStream()).thenReturn(byteStream);

        GraphiteVisitor visitor = new GraphiteVisitor() {
            @Override
            protected Socket createSocket() {
                return socket;
            }
        };

        visitor.visit(new JmxSample(Type.JVM, new ObjectName("java.lang:type=Runtime"), "uptime", Integer.valueOf(1), 1));

        assertThat(byteStream.toByteArray(), equalTo("cassandra.jvm.uptime 1 1\n".getBytes()));

        visitor.close();

    }

}
