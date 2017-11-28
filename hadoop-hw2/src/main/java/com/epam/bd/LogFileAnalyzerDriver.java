package com.epam.bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogFileAnalyzerDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set(Utils.ENABLE_DEBUG, System.getenv(Utils.ENABLE_DEBUG));
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapred.output.compression.type", "BLOCK");

        Job job = Job.getInstance(conf, "log analysis");
        job.setJarByClass(LogFileAnalyzerDriver.class);
        job.setMapperClass(LogFileAnalyzerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AddressMetric.class);
        job.setCombinerClass(LogFileAnalyzerCombiner.class);
        job.setReducerClass(LogFileAnalyzerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        //Print counters
        Utils.printUserAgentStats(job.getCounters().getGroup(Utils.COUNTER_GROUP));

        System.exit(success ? 0 : 1);
    }
}