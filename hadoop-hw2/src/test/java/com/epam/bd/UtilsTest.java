package com.epam.bd;

import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {
    public static String LOG1 = "ip465 - - [25/Apr/2011:10:06:00 -0400] \"GET /favicon.ico HTTP/1.1\" 200 318 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; Tablet PC 2.0; MALC)\"";
    public static String LOG2 = "ip1 - - [27/Apr/2011:16:42:09 -0400] \"GET /sgi_indigo/indigo_backplane.jpg HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    public static String LOG3 = "Wrong record";
    public static String USER_AGENT1 = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; Tablet PC 2.0; MALC)";
    public static String USER_AGENT2 = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.50 Safari/534.24";
    public static String USER_AGENT3 = "Mozilla/5.0 (Windows; U; Windows NT 6.1; de; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8";
    public static String USER_AGENT4 = "Incorrect";

    @Test
    public void calculateAverage() throws Exception {
        long totalBytes = 123456;
        int count = 55;

        Assert.assertEquals(2244.65, Utils.calculateAverage(totalBytes, count), 0.0);
    }

    @Test
    public void getUserAgentName() throws Exception {
        Assert.assertEquals("Internet Explorer 8", Utils.getUserAgentName(USER_AGENT1));
        Assert.assertEquals("Chrome 11", Utils.getUserAgentName(USER_AGENT2));
        Assert.assertEquals("Firefox 3", Utils.getUserAgentName(USER_AGENT3));
        Assert.assertEquals("Unknown", Utils.getUserAgentName(USER_AGENT4));
    }

    @Test
    public void parseRecord() throws Exception {
        Utils.LogEntry logEntry = new Utils.LogEntry();

        logEntry = Utils.parseRecord(logEntry, LOG1);
        Assert.assertEquals("ip465", logEntry.ip);
        Assert.assertEquals(318, logEntry.responseBytes);
        Assert.assertEquals("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; Tablet PC 2.0; MALC)", logEntry.userAgent);

        logEntry = Utils.parseRecord(logEntry, LOG2);
        Assert.assertEquals("ip1", logEntry.ip);
        Assert.assertEquals(0, logEntry.responseBytes);
        Assert.assertEquals("Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)", logEntry.userAgent);


        logEntry = Utils.parseRecord(logEntry, LOG3);
        Assert.assertEquals(null, logEntry.ip);
        Assert.assertEquals(0, logEntry.responseBytes);
        Assert.assertEquals(null, logEntry.userAgent);

        logEntry = null;
    }

}