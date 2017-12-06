package com.epam.bd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ImpressionReducer extends Reducer<LongWritable, ImpressionEntry, Text, IntWritable> {
    private ImpressionEntry entry = new ImpressionEntry();
    private Text cityName = new Text();
    private IntWritable bidCount = new IntWritable();

    public void reduce(LongWritable key, Iterable<ImpressionEntry> values,
                       Context context
    ) throws IOException, InterruptedException {
        entry.set(key.get(), 0, 0, "");

        for (ImpressionEntry val : values) {
            if (val.getBiddingPrice() > 250) {
                entry.aggregate(val);
            }
        }

        cityName.set(Utils.lookupCity(entry.getCityId()));
        bidCount.set(entry.getCount());
        context.write(cityName, bidCount);
    }
}
