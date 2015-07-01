cassandra-metrics-collector
===========================

Build
-----
    $ mvn package

Run
---
    $ java -jar cassandra-metrics-collector-<version>-jar-with-dependencies.jar \
            cassandra.host.net \
            7199 \
            graphite.host.net \
            2003

Environment
-----------
Set `DRY_RUN=true` to write samples to stdout.