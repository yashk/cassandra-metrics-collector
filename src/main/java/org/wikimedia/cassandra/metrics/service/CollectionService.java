package org.wikimedia.cassandra.metrics.service;

import com.github.rvesse.airline.parser.ParseException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.wikimedia.cassandra.metrics.Discovery;
import org.wikimedia.cassandra.metrics.Filter;

import javax.management.remote.JMXServiceURL;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

/**
 * Created by ykochare on 9/7/16.
 */

@SpringBootApplication
public class CollectionService implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(CollectionService.class);
    private static final String FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    @Autowired
    private Config config;


    public static void main(String[] args) {

        try {
            SpringApplication.run(CollectionService.class, args);
        } catch (ParseException e) {
            System.err.println(e.getLocalizedMessage());
        } catch (Throwable e) {
            System.err.println("Unexpected error: " + e.getLocalizedMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }

    }


    private static Trigger newTrigger(String instance, int interval) {
        return TriggerBuilder.newTrigger()
                .withIdentity(triggerName(instance), "collectionGroup")
                .startNow()
                .withSchedule(simpleSchedule().withIntervalInSeconds(interval).repeatForever())
                .build();
    }


    private static String triggerName(String instance) {
        return String.format("%s_Trigger", instance);
    }


    public void run(String... args) {


        LOG.info("Starting up  with " + config);


        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

            LOG.info("Initiating new discovery cycle", this);
            Discovery.Jvm jvm = new Discovery.Jvm(config.getCarbon().getPrefix(), new JMXServiceURL(String.format(FORMAT_URL, config.getJmx().getHost(), config.getJmx().getPort())));

            LOG.info("Found instance {}", jvm.getCassandraInstance());
            LOG.debug("Verifying JMX connectivity...");


            JobDataMap dataMap = new JobDataMap();
            dataMap.put("jvm", jvm);
            dataMap.put("carbonHost", config.getCarbon().getHost());
            dataMap.put("carbonPort", config.getCarbon().getPort());
            dataMap.put("instanceName", jvm.getCassandraInstance());
            dataMap.put("filter", new Filter(config.getFilterConfig()));
            dataMap.put("interval", config.getInterval());

            JobDetail job = JobBuilder.newJob(Collector.class)
                    .withIdentity(jvm.getCassandraInstance(), "collectionGroup")
                    .usingJobData(dataMap)
                    .build();

            LOG.debug("Scheduling recurring metrics collection for {}", jvm.getCassandraInstance());

            scheduler.scheduleJob(job, newTrigger(jvm.getCassandraInstance(), config.getInterval()));

            // Triggers reporting of internal metrics
            Trigger reportTrigger = TriggerBuilder.newTrigger()
                    .withIdentity("reportTrigger", "reportGroup")
                    .startNow()
                    .withSchedule(simpleSchedule().withIntervalInSeconds(config.getInterval()).repeatForever())
                    .build();


            // The collection listener monitors for task completion in the collection
            // group, and updates the stats counters accordingly.
            Stats stats = new Stats();
            CollectionListener listener = new CollectionListener(scheduler, stats);
            scheduler.getListenerManager().addJobListener(listener, jobGroupEquals("collectionGroup"));

            // The stats job periodically reports internal stats to Graphite.
            JobDataMap statsMap = new JobDataMap();
            statsMap.put("carbonHost", config.getCarbon().getHost());
            statsMap.put("carbonPort", config.getCarbon().getPort());
            statsMap.put("stats", stats);
            statsMap.put("interval", config.getInterval());

            JobDetail statsJob = newJob(StatsReporter.class)
                    .withIdentity("reporterJob", "reportGroup")
                    .usingJobData(statsMap)
                    .build();

            scheduler.scheduleJob(statsJob, reportTrigger);
            scheduler.start();


        } catch (Throwable e) {
            LOG.error("Unexpected exception during discovery", e);
        }
    }

}
