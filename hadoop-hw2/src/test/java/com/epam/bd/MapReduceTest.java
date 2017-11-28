package com.epam.bd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mrunit.mapreduce.*;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MapReduceTest {


    MapDriver<LongWritable, Text, Text, AddressMetric> mapDriver;
    ReduceDriver<Text, AddressMetric, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, AddressMetric, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        LogFileAnalyzerMapper mapper = new LogFileAnalyzerMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, AddressMetric>();
        mapDriver.setMapper(mapper);

        LogFileAnalyzerReducer reducer = new LogFileAnalyzerReducer();
        reduceDriver = new ReduceDriver<Text, AddressMetric, Text, Text>();
        reduceDriver.setReducer(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip153 - - [24/Apr/2011:13:22:10 -0400] \"GET /~techrat/vw_spotters/vw_411_wagen_f.jpg HTTP/1.1\" 200 15909 \"http://host2/~techrat/vw_spotters/\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; pl; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\""));
        mapDriver.withInput(new LongWritable(), new Text(
                "ip153 - - [24/Apr/2011:13:22:15 -0400] \"GET /~techrat/vw_spotters/vw_412_cool.jpg HTTP/1.1\" 200 15107 \"http://host2/~techrat/vw_spotters/\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; pl; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\""));
        mapDriver.withInput(new LongWritable(), new Text(
                "ip32 - - [24/Apr/2011:13:23:08 -0400] \"GET /sgiFAQ/ HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)\""));
        mapDriver.withOutput(new Text("ip153"), new AddressMetric(15909, 1));
        mapDriver.withOutput(new Text("ip153"), new AddressMetric(15107, 1));
        mapDriver.withOutput(new Text("ip32"), new AddressMetric(0, 1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        reduceDriver.withInput(new Text("ip50"), Arrays.asList(
                new AddressMetric(500, 2),
                new AddressMetric(1000, 1)
            ));
        reduceDriver.withInput(new Text("ip30"), Arrays.asList(
                new AddressMetric(1000, 1)));
        reduceDriver.withInput(new Text("ip40"), Arrays.asList(
                new AddressMetric(0, 1),
                new AddressMetric(999, 3)
        ));
        reduceDriver.withOutput(new Text("ip50"), new Text("500.0,1500"));
        reduceDriver.withOutput(new Text("ip30"), new Text("1000.0,1000"));
        reduceDriver.withOutput(new Text("ip40"), new Text("249.75,999"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip160 - - [24/Apr/2011:13:35:11 -0400] \"GET /sun_ss5/ss5-newfan2.jpg HTTP/1.1\" 200 29609 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; de; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip160 - - [24/Apr/2011:13:35:11 -0400] \"GET /sun_ss5/ss5_rear.jpg HTTP/1.1\" 200 42643 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; de; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip163 - - [24/Apr/2011:13:38:28 -0400] \"GET /~techrat/ratrod/3_tb.jpg HTTP/1.1\" 200 38977 \"http://host2/~techrat/ratrod/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip163 - - [24/Apr/2011:13:38:28 -0400] \"GET /~techrat/ratrod/2_tb.jpg HTTP/1.1\" 200 45481 \"http://host2/~techrat/ratrod/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip13 - - [24/Apr/2011:13:36:12 -0400] \"GET /robots.txt HTTP/1.1\" 404 286 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""));
        mapReduceDriver.withOutput(new Text("ip13"), new Text("286.0,286"));
        mapReduceDriver.withOutput(new Text("ip160"), new Text("36126.0,72252"));
        mapReduceDriver.withOutput(new Text("ip163"), new Text("42229.0,84458"));
        mapReduceDriver.runTest();
    }
 }