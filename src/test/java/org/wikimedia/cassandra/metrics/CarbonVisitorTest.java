/* Copyright 2015 Eric Evans <eevans@wikimedia.org>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import com.google.common.base.Optional;

public class CarbonVisitorTest {

    @Test
    public void test() throws IOException, MalformedObjectNameException {

        final Socket socket = mock(Socket.class);
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        when(socket.getOutputStream()).thenReturn(byteStream);

        final CarbonConnector carbon = new CarbonConnector() {
            @Override
            protected Socket createSocket() {
                return socket;
            }
        };

        CarbonVisitor visitor = new CarbonVisitor(carbon, "cassandra", Optional.<Filter>absent());

        visitor.visit(new JmxSample(Type.JVM, new ObjectName("java.lang:type=Runtime"), "uptime", Integer.valueOf(1), 1));

        assertThat(byteStream.toByteArray(), equalTo("cassandra.jvm.uptime 1 1\n".getBytes()));

        visitor.close();

    }

}
