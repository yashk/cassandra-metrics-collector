/* Copyright 2015 Eric Evans <eevans@wikimedia.org> and Wikimedia Foundation
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

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikimedia.cassandra.metrics.CarbonConnector;

public class StatsReporter implements Job {

    private static final String SUCCESS = "cmcd.instances.%s.success";
    private static final String FAILURE = "cmcd.instances.%s.failure";

    private static final Logger LOG = LoggerFactory.getLogger(StatsReporter.class);

    private Stats stats;
    private String carbonHost;
    private int carbonPort;
    private int interval;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOG.debug("Connecting to {}:{}", this.carbonHost, this.carbonPort);

        try (final CarbonConnector carbon = new CarbonConnector(this.carbonHost, this.carbonPort)) {
            LOG.info("Writing internal stats");

            new TimedTask<Void>(Math.min(interval, 60)).submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (String instance : stats.getNames()) {
                        carbon.write(String.format(FAILURE, instance), stats.getFailures(instance));
                        carbon.write(String.format(SUCCESS, instance), stats.getSuccesses(instance));
                    }
                    return null;
                }
            });
        }
        catch (Throwable e) {
            LOG.error("Unable to report internal stats", e);
        }
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public void setCarbonHost(String carbonHost) {
        this.carbonHost = carbonHost;
    }

    public void setCarbonPort(int carbonPort) {
        this.carbonPort = carbonPort;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

}
