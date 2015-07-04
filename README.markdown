cassandra-metrics-collector
===========================

Collects Cassandra performance metrics (via JMX) and writes them to Graphite in a
format compatible with the Dropwizard metrics GraphiteReporter.

Build
-----
    $ mvn package

Run
---
    $ java -jar cassandra-metrics-collector-<version>-jar-with-dependencies.jar \
            cassandra.host.net \
            7199 \
            graphite.host.net \
            2003 \
            servers.cassandra.host

Environment
-----------
Set `DRY_RUN=true` to write samples to stdout.