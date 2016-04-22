cassandra-metrics-collector [![Build Status](https://travis-ci.org/wikimedia/cassandra-metrics-collector.svg?branch=master)](https://travis-ci.org/wikimedia/cassandra-metrics-collector)
===========================

*Note: This branch supports Cassandra >= 2.2 (tested with 2.2.5).  For Cassandra 2.1,
please use [this branch](https://github.com/wikimedia/cassandra-metrics-collector/tree/2.0.0).*

Discovers running instances of Cassandra on the local machine, collects
performance metrics (via JMX, using a domain socket), and writes them to
[Graphite](https://github.com/graphite-project/graphite-web)/[Carbon](https://github.com/graphite-project/carbon)
in a format compatible with the [Dropwizard metrics](http://metrics.dropwizard.io)
GraphiteReporter.

This tool is capable of discovering and collecting from an arbitrary number
of local Cassandra instances, and so it is required that you disambiguate
instances by passing a `-Dcassandra.instance-id=<name>` system property to
Cassandra at startup.  This instance name will be used to derive the metric
prefix string unique to each instance, (in the form of `cassandra.<name>.`).

Build
-----
    $ mvn package

Run
---
    $ java -jar target/cassandra-metrics-collector-<version>-jar-with-dependencies.jar --help
    
    NAME
                cmcd - cassandra-metrics-collector daemon
    
    SYNOPSIS
            cmcd [ {-h | --help} ] [ {-H | --carbon-host} <HOSTNAME> ]
                            [ {-i | --interval} <INTERVAL> ] [ {-p | --carbon-port} <PORT> ]
    
    OPTIONS
            -h, --help
                        Display help information
    
            -H <HOSTNAME>, --carbon-host <HOSTNAME>
                        Carbon hostname (default: localhost)
    
            -i <INTERVAL>, --interval <INTERVAL>
                        Collection interval in seconds (default: 60 seconds)
    
            -p <PORT>, --carbon-port <PORT>
                        Carbon port number (default: 2003)

For example:

    $ export CLASSPATH=/path/to/apache-cassandra.jar:/path/to/cassandra-metrics-collector-<version>-jar-with-dependencies.jar
    $ java org.wikimedia.cassandra.metrics.service.Service \
            --interval 60 \
            --carbon-host carbon-1.example.com \
            --carbon-port 2003 \


Simple invocation
-----------------
It is also possible to invoke a single collection cycle against a specific
instance over TCP.

    $ export CLASSPATH=/path/to/apache-cassandra.jar:/path/to/cassandra-metrics-collector-<version>-jar-with-dependencies.jar
    $ java org.wikimedia.cassandra.metrics.Command
    Usage: Command <jmx host> <jmx port> <graphite host> <graphite port> <prefix>

For example:

    $ export CLASSPATH=/path/to/apache-cassandra.jar:/path/to/cassandra-metrics-collector-<version>-jar-with-dependencies.jar
    $ java -cp org.wikimedia.cassandra.metrics.Command \
          db-1.example.com \
          7199 \
          carbon-1.example.com \
          2003 \
          cassandra.db-1


Testing locally
---------------
To test locally, use `nc` (netcat).

Open a socket and listen on port 2003:

    $ nc -lk 2003

Collect Cassandra metrics and write to netcat:

    $ export CLASSPATH=/path/to/apache-cassandra.jar:/path/to/cassandra-metrics-collector-<version>-jar-with-dependencies.jar
    $ java org.wikimedia.cassandra.metrics.service.Service \
          --interval 15 \
          --carbon-host localhost \
          --carbon-port 2003 \
