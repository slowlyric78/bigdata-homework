package com.epam.hive;

import org.apache.hadoop.io.Text;
import eu.bitwalker.useragentutils.UserAgent;

public class Utils {
    public static final String UNKNOWN = "Unknown";

    public static void parseUserAgent(Text input, Text device, Text os, Text browser, Text userAgent)
    {
        try {
            UserAgent ua  = UserAgent.parseUserAgentString(input.toString());

            device.set(ua.getOperatingSystem().getDeviceType().getName());
            os.set(ua.getOperatingSystem().getName());
            browser.set(ua.getBrowser().getName());
            userAgent.set(ua.getBrowser().getBrowserType().getName());
        } catch(IllegalArgumentException ex) {
            System.out.println("Cannot identify user agent: " + input);

            device.set(UNKNOWN);
            os.set(UNKNOWN);
            browser.set(UNKNOWN);
            userAgent.set(UNKNOWN);
        }
    }
}
