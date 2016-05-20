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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimedTask<T> {

    private final int timeout;

    /**
     * Creates a new {@link TimedTask} instance for a given timeout.
     *
     * @param timeout
     *            the timeout, in seconds
     */
    public TimedTask(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Executes a {@link Callable}, returning the result, or raising an exception on timeout.
     * 
     * @param callable
     *            the callable to execute
     * @return
     * @throws TimedTaskException
     */
    public T submit(Callable<T> callable) throws TimedTaskException {

        T result = null;
        ExecutorService service = Executors.newSingleThreadExecutor();
        Future<T> future = service.submit(callable);

        try {
            result = future.get(this.timeout, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            future.cancel(true);
            throw new TimedTaskException("Task execution interrupted", e);
        }
        catch (ExecutionException e) {
            future.cancel(true);
            throw new TimedTaskException("Task execution exception", e);
        }
        catch (TimeoutException e) {
            future.cancel(true);
            throw new TimedTaskException(String.format("Timeout of %d seconds exceeded", this.timeout));
        }
        finally {
            service.shutdown();
        }

        return result;
    }
}
