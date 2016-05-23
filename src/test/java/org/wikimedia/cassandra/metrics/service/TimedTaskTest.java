/* Copyright 2016 Eric Evans <eevans@wikimedia.org> and Wikimedia Foundation
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
package org.wikimedia.cassandra.metrics.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.wikimedia.cassandra.metrics.CarbonException;

public class TimedTaskTest {

    @Test
    public void test() throws TimedTaskException {
        final AtomicBoolean called = new AtomicBoolean(false);

        new TimedTask<Void>(1).submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                called.set(true);
                return null;
            }
        });

        assertThat(called.get(), is(true));
    }

    @Test
    public void testException() {
        try {
            new TimedTask<Void>(1).submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    throw new CarbonException("oh noes");
                }
            });
        }
        catch (TimedTaskException e) {
            assertThat(e.getCause(), instanceOf(ExecutionException.class));
            assertThat(e.getCause().getCause(), instanceOf(CarbonException.class));
            return;
        }

        fail("TimedTask failed to throw expected exception");
    }

    @Test
    public void testTimeout() {
        try {
            new TimedTask<Void>(1).submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Thread.sleep(1100);
                    return null;
                }
            });

            fail("TimedTask failed to throw expected exception");
        }
        catch (TimedTaskException e) {
            assertThat(e.getCause(), equalTo(null));
            return;
        }

    }

}
