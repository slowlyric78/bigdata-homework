package com.epam.bd;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class LogFileAnalyzerReducer
    extends Reducer<Text, AddressMetric, Text, Text> {

    private Text ip = new Text();
    private Text textMetric = new Text();
    private AddressMetric metric = new AddressMetric();

    /**
     * Network log data aggregator
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<AddressMetric> values,
                       Context context
    ) throws IOException, InterruptedException {
        boolean enableDebug = Boolean.valueOf(
                context.getConfiguration().get(Utils.ENABLE_DEBUG));
        if (enableDebug) System.out.println("key: " + key.toString());

        metric.set(0, 0);
        for (AddressMetric val : values) {
            if (enableDebug) System.out.println("value: " + val);

            metric.aggregate(val);
        }

        ip.set(key);
        textMetric.set(metric.average() + "," + metric.getTotalBytes());
        context.write(ip, textMetric);
    }
}
