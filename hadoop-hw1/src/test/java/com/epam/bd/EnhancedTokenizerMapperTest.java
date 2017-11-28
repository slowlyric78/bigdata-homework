package com.epam.bd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mrunit.mapreduce.*;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class EnhancedTokenizerMapperTest {

    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
    ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        LongestWordMR.EnhancedTokenizerMapper mapper = new LongestWordMR.EnhancedTokenizerMapper();
        mapDriver = new MapDriver<LongWritable, Text, IntWritable, Text>();
        mapDriver.setMapper(mapper);

        LongestWordMR.LongWordReducer reducer = new LongestWordMR.LongWordReducer();
        reduceDriver = new ReduceDriver<IntWritable, Text, IntWritable, Text>();
        reduceDriver.setReducer(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }


    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text("cat dog pig tiger sheep unicorn"));
        mapDriver.withInput(new LongWritable(10), new Text("carrot apple orange plum prune"));
        mapDriver.withOutput(new IntWritable(1), new Text("unicorn"));
        mapDriver.withOutput(new IntWritable(1), new Text("carrot"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        reduceDriver.withInput(new IntWritable(1), Arrays.asList(
                    new Text("one"), new Text("five"), new Text("eleven"), new Text("ten")));
        reduceDriver.withOutput(new IntWritable(6), new Text("eleven"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text("Quick brown fox jumped over the lazy dog"));
        mapReduceDriver.withInput(new LongWritable(), new Text("Window washers washed Washington's windows"));
        mapReduceDriver.withOutput(new IntWritable(12), new Text("Washington's"));
        mapReduceDriver.runTest();
    }
}