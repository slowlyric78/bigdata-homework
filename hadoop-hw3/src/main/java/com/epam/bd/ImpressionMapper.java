package com.epam.bd;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Impression mapper
 */
public class ImpressionMapper extends Mapper<LongWritable, Text, LongWritable, ImpressionEntry>  {
    private static final Logger log = LogManager.getLogger(ImpressionMapper.class);

    LongWritable cityId = new LongWritable();
    ImpressionEntry entry = new ImpressionEntry();

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {

        Utils.parseLine(entry, value.toString());

        if (entry.getCityId() >= 0) {
            cityId.set(entry.getCityId());
            context.write(cityId, entry);
        }
    }
}
