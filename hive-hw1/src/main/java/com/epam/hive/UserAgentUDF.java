package com.epam.hive;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Arrays;

@Description(
        name = "useragent",
        value = "_FUNC_(string) - parses user agent input string"
)public class UserAgentUDF extends UDF {
    private Text device = new Text();
    private Text os = new Text();
    private Text browser = new Text();
    private Text userAgent = new Text ();

    public ArrayList<Text> evaluate (Text input) {
        Utils.parseUserAgent(input, device, os, browser, userAgent);

        return new ArrayList<Text>(Arrays.asList(device, os, browser, userAgent));
    }
}
