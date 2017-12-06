package com.epam.bd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mrunit.mapreduce.*;

import java.util.Arrays;

import static org.junit.Assert.*;


public class MapReduceTest {
    MapDriver<LongWritable, Text, LongWritable, ImpressionEntry> mapDriver;
    ReduceDriver<LongWritable, ImpressionEntry, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, LongWritable, ImpressionEntry, Text, IntWritable> mapReduceDriver;

    String line1 = "445ed8056d8279f5419dc74dbc1a71bc\t20131019125009566\t1\tD11DNV9De8M\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; KB974488)\t27.38.216.*\t216\t219\t3\t7ed515fe566938ee6cfbb6ebb7ea4995\t4e613a7394f0f4c998904fc932d7a15c\tnull\tSports_F_Upright\t300\t250\tNa\tNa\t10\t7323\t294\t11\tnull\t2259\t13800,10059,10684,10075,10083,13042,10102,10024,10006,10111,11944,13403,10133,10063,10116,10125\n";
    String line2 = "b5d859a10a3a5588204f8b8e35ead2f\t20131019131808811\t1\tD12FlHCie6g\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t113.67.164.*\t216\t219\t3\tdd4270481b753dde29898e27c7c03920\t7d327804a2476a668c633b7fb857fddb\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t100\t255\tnull\t2259\t10048,10057,10067,10059,13496,10077,10093,10075,13042,10102,10006,10024,10148,10031,13776,10111,10127,10063,10116\n";
    String line3 = "34381416d77d8c572a98f03e6cc8b84d\t20131019173100595\t1\tD1EMul2gcUr\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t183.51.230.*\t216\t216\t2\tc80deeed1b57eaed72ec51235e21bf89\t1b3f1adf943b9c7b44dcedd76f5a654d\tnull\t4186967039\t728\t90\tFirstView\tNa\t5\t7330\t201\t139\tnull\t2259\t10057,10059,10076,10093,10075,10118,10083,10074,10006,10024,10148,11423,10110,10131,10063,10125\n";
    String line4 = "b6b39e23dcf1857e9b6a67989853af48\t20131019131719059\t1\tD54JF8CAFMn\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t183.44.82.*\t216\t216\t3\tdd4270481b753dde29898e27c7c03920\t14d61cc01a4101179a8c540fa68305d1\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t294\t70\tnull\t2259\t10006,13403,10063,10116\n";
    String line5 = "8f13a6c667e94c733937b72fc318c6ac\t20131019164600959\t1\tD54NVED4Gkx\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.81.120.*\t216\t216\t2\t6719df73b092f3cf8084106111dc1053\tadbfb6b14c1992f14439a4abfb703fef\tnull\t3641599401\t300\t250\tNa\tNa\t4\t7323\t277\t11\tnull\t2259\t10006,13403,10115,10063\n";
    String line6 = "ab551c27a0f1021e312c471554e2b12\t20131019040200623\t1\tD5QEL_0K6Gv\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t211.148.89.*\t216\t210\t2\t36b1c4d8658af5440ad6657a0fa74c0a\t27c11a39f54d02357a1d92f047faa5f4\tnull\t443702222\t728\t90\tOtherView\tNa\t5\t7330\t277\t44\tnull\t2259\t13800,10059,10077,10006,13866,10111,10131,10115,10063\n";
    String line7 = "fad1673c20362f6f244adca4234ecd7f\t20131019125401029\t1\tD5SAHG1PbFK\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; youxihe.1748)\t183.4.178.*\t216\t210\t1\tf46fd165e29be3996347d1f4357ceaf5\t805f53d277437b89ba45f76164308dc7\tnull\tmm_13181159_1847728_9696846\t950\t90\tFirstView\tNa\t0\t7333\t294\t228\tnull\t2259\t10048,10057,10059,10075,10093,10083,13042,10006,10024,10110,10031,10131,10052,13403,10063,10116,10125\n";
    String DUMMY_AGENT = "dummy-agent";

    @Before
    public void setUp() {
        ImpressionMapper mapper = new ImpressionMapper();
        mapDriver = new MapDriver<LongWritable, Text, LongWritable, ImpressionEntry>();
        mapDriver.setMapper(mapper);

        ImpressionReducer reducer = new ImpressionReducer();
        reduceDriver = new ReduceDriver<LongWritable, ImpressionEntry, Text, IntWritable>();
        reduceDriver.setReducer(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text(line1));
        mapDriver.withInput(new LongWritable(), new Text(line2));
        mapDriver.withInput(new LongWritable(), new Text(line3));
        mapDriver.withInput(new LongWritable(), new Text(line4));
        mapDriver.withInput(new LongWritable(), new Text(line5));
        mapDriver.withOutput(new LongWritable(219), new ImpressionEntry(219, 294, 1,
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; KB974488)"));
        mapDriver.withOutput(new LongWritable(219), new ImpressionEntry(219, 100, 1,
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)"));
        mapDriver.withOutput(new LongWritable(216), new ImpressionEntry(216, 201, 1,
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"));
        mapDriver.withOutput(new LongWritable(216), new ImpressionEntry(216, 294, 1,
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"));
        mapDriver.withOutput(new LongWritable(216), new ImpressionEntry(216, 277, 1,
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        reduceDriver.withInput(new LongWritable(10), Arrays.asList(
                new ImpressionEntry(10, 200,1, DUMMY_AGENT),
                new ImpressionEntry(10, 250,1, DUMMY_AGENT),
                new ImpressionEntry(10,300,1, DUMMY_AGENT),
                new ImpressionEntry(10,350,1, DUMMY_AGENT),
                new ImpressionEntry(10,500,1, DUMMY_AGENT)));
        reduceDriver.withInput(new LongWritable(20), Arrays.asList(
                new ImpressionEntry(20,50,1, DUMMY_AGENT),
                new ImpressionEntry(20,250,1, DUMMY_AGENT),
                new ImpressionEntry(20,500,1, DUMMY_AGENT)));
        reduceDriver.withInput(new LongWritable(30), Arrays.asList(
                new ImpressionEntry(30,100,1, DUMMY_AGENT),
                new ImpressionEntry(30,500,1, DUMMY_AGENT)));

        reduceDriver.withOutput(new Text("zhangjiakou"), new IntWritable(3));
        reduceDriver.withOutput(new Text("jincheng"), new IntWritable(1));
        reduceDriver.withOutput(new Text("wuhai"), new IntWritable(1));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text(line1));
        mapReduceDriver.withInput(new LongWritable(), new Text(line2));
        mapReduceDriver.withInput(new LongWritable(), new Text(line3));
        mapReduceDriver.withInput(new LongWritable(), new Text(line4));
        mapReduceDriver.withInput(new LongWritable(), new Text(line5));
        mapReduceDriver.withInput(new LongWritable(), new Text(line6));
        mapReduceDriver.withInput(new LongWritable(), new Text(line7));

        mapReduceDriver.withOutput(new Text("yiyang"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("missing(216)"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("shenzhen"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
