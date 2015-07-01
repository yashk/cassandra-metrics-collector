package org.wikimedia.cassandra.metrics;

import java.io.IOException;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Ignore;
import org.junit.Test;

public class JmxCollectorTest {

    @Ignore
    @Test
    public void test() throws IOException, MalformedObjectNameException {
        JmxCollector col = new JmxCollector();
        String[] inputs = new String[] {
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system_traces,scope=events,name=MemtableLiveDataSize",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=peer_events,name=EstimatedRowCount",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=peers,name=BloomFilterFalsePositives",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system_traces,scope=events,name=SpeculativeRetries",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=IndexInfo,name=EstimatedColumnCountHistogram",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=schema_triggers,name=BloomFilterFalsePositives",
                "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=Sampler,name=MaxPoolSize",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=hints,name=PendingCompactions",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=peers,name=CompressionRatio",
                "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=system,scope=schema_usertypes,name=LiveDiskSpaceUsed",
                "org.apache.cassandra.metrics:type=Connection,scope=127.0.0.1,name=CommandCompletedTasks" };
        for (String input : inputs)
            System.out.println(col.graphiteName(new ObjectName(input)));
    }
}
