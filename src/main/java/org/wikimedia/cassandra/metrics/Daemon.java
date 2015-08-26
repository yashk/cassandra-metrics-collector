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
package org.wikimedia.cassandra.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rvesse.airline.Command;
import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.Option;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.parser.ParseException;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

@Command(name = "cmcd", description = "cassandra-metrics-collector daemon")
public class Daemon {

    static class Collector implements Runnable {
        private final Daemon daemon;
        private final Discovery.Jvm jvm;

        Collector(Daemon daemon, Discovery.Jvm jvm) {
            this.daemon = daemon;
            this.jvm = jvm;
        }

        @Override
        public void run() {
            LOG.info("Collecting from {}", getInstance());
            LOG.debug("Sending metrics to {}:{} using prefix {}", getGraphiteHost(), getGraphitePort(), getPrefix());

            try (JmxCollector c = new JmxCollector(getJmxUrl())) {
                try (GraphiteVisitor v = new GraphiteVisitor(getGraphiteHost(), getGraphitePort(), getPrefix())) {
                    c.getSamples(v);
                }
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage());
                return;
            } catch (GraphiteException e) {
                LOG.error("Unable to write samples to Carbon: {}", e.getLocalizedMessage());
                return;
            } catch (Throwable e) {
                LOG.error("Unexpected error", e);
                return;
            }

            LOG.debug("Collection of {} complete!", getInstance());
        }

        String getGraphiteHost() {
            return this.daemon.getGraphiteHost();
        }

        int getGraphitePort() {
            return this.daemon.getGraphitePort();
        }

        String getPrefix() {
            return prefix(getInstance());
        }

        JMXServiceURL getJmxUrl() {
            return this.jvm.getJmxUrl();
        }

        String getInstance() {
            return this.jvm.getCassandraInstance();
        }
    }

    public static final String PREFIX_PREFIX = "cassandra";

    private static final Logger LOG = LoggerFactory.getLogger(Daemon.class);

    @Inject
    private HelpOption help;

    @Option(name = { "-H", "--carbon-host" }, description = "Carbon hostname (default: localhost)", title = "HOSTNAME")
    private String graphiteHost = "localhost";

    @Option(name = { "-p", "--carbon-port" }, description = "Carbon port number (default: 2003)", title = "PORT")
    private int graphitePort = 2003;

    @Option(name = { "-i", "--interval" }, description = "Collection interval in seconds (default: 60 seconds)", title = "INTERVAL")
    private int interval = 60;

    void execute() throws IOException {

        // Print a synopsis to STDOUT (if requested), and exit.
        if (showHelpIfRequested()) {
            return;
        }

        LOG.info("Starting up...");

        Collection<Discovery.Jvm> jvms = new Discovery().getJvms().values();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(jvms.size());
        List<ScheduledFuture<?>> futures = Lists.newArrayList();

        for (Discovery.Jvm jvm : jvms) {
            String name = jvm.getCassandraInstance();
            LOG.info("Discovered instance named {} (prefix {})", name, prefix(name));
            futures.add(executor.scheduleAtFixedRate(new Collector(this, jvm), 0, this.interval, TimeUnit.SECONDS));
        }

        // Short of an executor shutdown (which nothing currently does), these futures
        // should block indefinitely unless there is an exception.
        for (ScheduledFuture<?> f : futures) {
            try {
                f.get();
            } catch (Throwable e) {
                LOG.error("Unexpected exception executing scheduled collection", e);
                throw Throwables.propagate(e);
            }
        }

        // XXX: What would it take to reach here?
        LOG.info("Exiting...");

    }

    boolean showHelpIfRequested() {
        return this.help.showHelpIfRequested();
    }

    String getGraphiteHost() {
        return graphiteHost;
    }

    int getGraphitePort() {
        return graphitePort;
    }

    public static void main(String[] args) {

        // We need tools.jar as shipped with the JDK (to power discovery). The
        // following attempts to locate the jar and add it to the classpath.
        boolean added = false;
        try {
            added = Utils.addToolsJar();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.getCause().printStackTrace(System.err);
            System.exit(1);
        }

        if (!added) {
            System.err.println("Unable to locate tools.jar (hint: do you have a JDK installed?");
            System.exit(1);
        }

        SingleCommand<Daemon> parser = SingleCommand.singleCommand(Daemon.class);

        try {
            parser.parse(args).execute();

        } catch (ParseException e) {
            System.err.println(e.getLocalizedMessage());
        } catch (Throwable e) {
            System.err.println("Unexpected error: " + e.getLocalizedMessage());
            e.printStackTrace(System.err);
        }

    }

    private static String prefix(String id) {
        return String.format("%s.%s", PREFIX_PREFIX, id);
    }

}
