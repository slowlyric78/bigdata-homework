package com.epam.bd;

import eu.bitwalker.useragentutils.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static final String COUNTER_GROUP = "User Agent";
    public static final String ENABLE_DEBUG = "ENABLE_DEBUG";
    public static final String UNKNOWN_USER_AGENT = "Unknown";
    private static Pattern regex = Pattern.compile(
            "(.*?)\\s.*\\[(.*)\\].*\\d\\d\\d\\s([0-9\\-]+)\\s\\\".*\\\"\\s\\\"(.*)\\\"");

    public static class LogEntry {
        public String ip;
        public long responseBytes;
        public String userAgent;
    }

    public static double calculateAverage(long total, int count)
    {
        double average = Math.round((double)total/count*100.0)/100.0;

        return average;
    }

    public static void printUserAgentStats(CounterGroup group)
    {
        System.out.println("User agent stats:");

        Iterator<Counter> iter = group.iterator();
        while (iter.hasNext())        {
            Counter c = iter.next();

            System.out.println(c.getName() + " : " + c.getValue());
        }
    }

    public static String getUserAgentName(String userAgent)
    {
        String name = null;

        try {
            name = UserAgent.parseUserAgentString(userAgent).getBrowser().getName();
        } catch(IllegalArgumentException ex) {
            System.out.println("Cannot identify user agent: " + userAgent);
            name = UNKNOWN_USER_AGENT;
        }

        return name;
    }

    public static LogEntry parseRecord (LogEntry logEntry, String logString)
    {
        Matcher m = regex.matcher(logString);

        if (m.matches())
        {
            logEntry.ip = m.group(1);
            try {
                logEntry.responseBytes = Long.valueOf(m.group(3));
            } catch (NumberFormatException ex) {
                System.out.println("Cannot parse number: " + m.group(3) + ", using '0'"
                        + ", log line: " + logString);
                logEntry.responseBytes = 0;
            }
            logEntry.userAgent = m.group(4);
        } else {
            System.out.println("Problem matching log entry: " + logString);

            logEntry.ip = null;
            logEntry.responseBytes = 0;
            logEntry.userAgent = null;
        }

        return logEntry;
    }

}
