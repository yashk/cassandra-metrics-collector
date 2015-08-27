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

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionListener implements JobListener {
    private static final Logger LOG = LoggerFactory.getLogger(CollectionListener.class);

    private final Scheduler scheduler;
    private final Stats stats;

    CollectionListener(Scheduler scheduler, Stats stats) {
        this.scheduler = scheduler;
        this.stats = stats;
    }

    @Override
    public String getName() {
        return "collection-listener";
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext arg0) {
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext arg0) {
    }

    @Override
    public void jobWasExecuted(JobExecutionContext ctx, JobExecutionException arg1) {
        Collector col = (Collector) ctx.getJobInstance();
        switch (col.getStatus()) {
        case SUCCESS:
            this.stats.success(col.getInstanceName());
            break;

        case FAILURE:
            this.stats.failure(col.getInstanceName());
            break;

        case ERROR:
            try {
                LOG.error("Collector for {} experienced unrecoverable error, descheduling...", col.getInstanceName());
                this.scheduler.deleteJob(ctx.getJobDetail().getKey());
            }
            catch (SchedulerException e) {
                LOG.error("Unexpected exception descheduling job", e);
            }
            this.stats.failure(col.getInstanceName());
            break;

        default:
            break;
        }
    }

}