package com.epam.bd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Log file analyzer mapper
 */
public class LogFileAnalyzerMapper
        extends Mapper<LongWritable, Text, Text, AddressMetric> {


    // ip43 - - [24/Apr/2011:06:37:14 -0400] "GET /faq/ HTTP/1.1" 200 11077 "-" "Java/1.6.0_04"
    // 1 - ip, 2 - timestamp, 3 - response bytes, 4 - user agent
    private Utils.LogEntry logEntry = new Utils.LogEntry();
    private Text ip = new Text();
    private AddressMetric metric = new AddressMetric();

    /**
     * Network log analyzer mapper using custom writable object
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        boolean enableDebug = Boolean.valueOf(
                    context.getConfiguration().get(Utils.ENABLE_DEBUG));

        if (enableDebug) System.out.println("key: " + key.toString() + ", value: "+ value.toString());

        logEntry = Utils.parseRecord(logEntry, value.toString());

        if (logEntry.ip != null) {
            ip.set(logEntry.ip);
            metric.set(logEntry.responseBytes, 1);
            context.write(ip, metric);
        }

        //todo: apply counters
        if (logEntry.userAgent != null) {
            String userAgentName = Utils.getUserAgentName(logEntry.userAgent);

            if (userAgentName != null) {
                context.getCounter(Utils.COUNTER_GROUP, userAgentName).increment(1);

                if (enableDebug) System.out.println("user agent name:" + userAgentName);
            }
        }

    }



}
