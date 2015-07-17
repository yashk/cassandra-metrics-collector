cassandra-metrics-collector
===========================

Collects Cassandra performance metrics (via JMX) and writes them to [Graphite](https://github.com/graphite-project/graphite-web)/[Carbon](https://github.com/graphite-project/carbon)
in a format compatible with the [Dropwizard metrics](http://metrics.dropwizard.io)
GraphiteReporter.

Build
-----
    $ mvn package

Run
---
    $ java -jar target/cassandra-metrics-collector-<version>-jar-with-dependencies.jar
    Usage: Command <jmx host> <jmx port> <carbon host> <carbon port> <prefix>

For example:
    
    $ java -jar cassandra-metrics-collector-<version>-jar-with-dependencies.jar \
            db-1.example.com \
            7199 \
            carbon-1.example.com \
            2003 \
            cassandra.db-1
    

Environment
-----------
Set `DRY_RUN=true` to write samples to stdout.