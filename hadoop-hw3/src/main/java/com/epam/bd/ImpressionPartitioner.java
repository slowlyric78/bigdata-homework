package com.epam.bd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User Agent based partitioner
 */
public class ImpressionPartitioner extends Partitioner<LongWritable,ImpressionEntry> {

    public int getPartition(LongWritable longWritable, ImpressionEntry impressionEntry, int numPartitions) {
        if (numPartitions == 0) return 0;

        return Utils.getPartitionId(impressionEntry.getUserAgent());
    }
}
