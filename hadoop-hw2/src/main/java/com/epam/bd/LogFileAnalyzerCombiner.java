package com.epam.bd;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class LogFileAnalyzerCombiner
    extends Reducer<Text, AddressMetric, Text, AddressMetric> {

    //private Text ip = new Text();
    private AddressMetric metric = new AddressMetric();

    /**
     * Basic metric data aggregator
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(Text key, Iterable<AddressMetric> values, Context context)throws IOException, InterruptedException {
        boolean enableDebug = Boolean.valueOf(
                context.getConfiguration().get(Utils.ENABLE_DEBUG));
        if (enableDebug) System.out.println("key: " + key);

        metric.set(0, 0);
        for (AddressMetric value : values) {
            if (enableDebug) System.out.println("value: " + value);

            metric.aggregate(value);
        }

        context.write(key, metric);
    }
}
